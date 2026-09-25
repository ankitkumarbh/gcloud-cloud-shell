import os
import sys
import json
import time
import secrets
import logging
import threading
import subprocess
import queue
import pty
import select
import fcntl
import termios
import struct
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from collections import deque
from flask import Flask, request, jsonify, Response, render_template_string, session, redirect, url_for, make_response
import zoneinfo

_TZ_IST = zoneinfo.ZoneInfo("Asia/Kolkata")

import mongo_store

_executor = ThreadPoolExecutor(max_workers=4)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("gcloud-shell")

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

PORT = int(os.environ.get("PORT", "10000"))
AUTH_PASSWORD = os.environ.get("AUTH_PASSWORD", "")
FAIL_THRESHOLD = int(os.environ.get("FAIL_THRESHOLD", "5"))
GCLOUD_CONFIG_DIR = os.path.expanduser("~/.config/gcloud")
KEEPALIVE_INTERVAL = 60

_log_buffer = deque(maxlen=5000)
_log_subscribers: list[queue.Queue] = []
_status_subscribers: list[queue.Queue] = []
_lock = threading.Lock()
_tmux_lock = threading.Lock()
_tmux_cache = {"output": "[no output]", "time": 0}
_session_info = {
    "current_account": None,
    "status": "initializing",
    "fail_count": 0,
    "uptime_start": time.time(),
    "last_disconnect": None,
    "running": False,
    "last_keepalive": None,
    "last_command": None,
    "last_exec_output": None,
    "last_exec_rc": None,
    "exec_running": False,
}

_keepalive_thread: threading.Thread | None = None
_keepalive_stop = threading.Event()


def _log(msg: str):
    ts = datetime.now(_TZ_IST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    logger.info(msg)
    with _lock:
        _log_buffer.append(line)
        dead = []
        for q in _log_subscribers:
            try:
                q.put_nowait(line)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _log_subscribers.remove(q)


def _update_status(**kwargs):
    with _lock:
        _session_info.update(kwargs)
        snapshot = {k: v for k, v in _session_info.items()}

        uptime = int(time.time() - snapshot["uptime_start"])
        days, rem = divmod(uptime, 86400)
        hours, rem = divmod(rem, 3600)
        mins, secs = divmod(rem, 60)
        snapshot["uptime_str"] = f"{days}d {hours}h {mins}m {secs}s"
        lk = snapshot.get("last_keepalive")
        if lk:
            ago = int(time.time() - lk)
            m, s = divmod(ago, 60)
            snapshot["last_keepalive_ago"] = f"{m}m {s}s ago"
        else:
            snapshot["last_keepalive_ago"] = "never"
        snapshot["ssh_connected"] = _shell_conn._is_alive()
        snapshot["ssh_host"] = _shell_conn.host or "-"

        dead = []
        for q in _status_subscribers:
            try:
                q.put_nowait(snapshot)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _status_subscribers.remove(q)


def _get_current_account():
    try:
        r = subprocess.run(
            ["gcloud", "config", "get-value", "account"],
            capture_output=True, text=True, timeout=15,
        )
        return r.stdout.strip()
    except Exception:
        return None


def _set_account(email: str):
    subprocess.run(
        ["gcloud", "config", "set", "account", email],
        capture_output=True, text=True, timeout=15,
    )
    _log(f"Switched gcloud account to: {email}")


def _get_next_account(current: str) -> str | None:
    accounts = mongo_store.get_accounts()
    if not accounts:
        return None
    emails = [a["email"] for a in accounts]
    if current in emails:
        idx = emails.index(current)
        return emails[(idx + 1) % len(emails)]
    return emails[0]


def _restore_from_mongo():
    _log("Restoring gcloud config from MongoDB...")
    try:
        mongo_store.restore_gcloud_config(GCLOUD_CONFIG_DIR)
    except Exception as e:
        _log(f"Restore failed: {e}")
        return
    default_account = mongo_store.get_default_account()
    if default_account:
        _set_account(default_account)
    else:
        accounts = mongo_store.get_accounts()
        if accounts:
            _set_account(accounts[0]["email"])


TERMUX_ACCOUNT_ORDER = [
    "ankitkumarbh@gmail.com",
    "aktechbh@gmail.com",
    "ankitkumarbh4@gmail.com",
    "mraj64646464@gmail.com",
]


def _ensure_account_order():
    db = mongo_store._get_db()
    for idx, email in enumerate(TERMUX_ACCOUNT_ORDER):
        db["accounts"].update_one(
            {"email": email},
            {"$set": {"sort_order": idx}},
            upsert=True,
        )
    _log("Account order synced with Termux script")


def _save_config_to_mongo():
    try:
        mongo_store.save_gcloud_config(GCLOUD_CONFIG_DIR)
    except Exception as e:
        _log(f"Save failed: {e}")


def _write_start_script():
    """Write start.sh from MongoDB to /app/start.sh for tunnel SCP."""
    content = mongo_store.get_start_script()
    if not content:
        _log("[start] No start.sh in MongoDB, creating minimal one")
        content = "#!/bin/bash\necho 'No start.sh configured'\n"
    with open("/app/start.sh", "w") as f:
        f.write(content)
    os.chmod("/app/start.sh", 0o755)
    _log("[start] Written /app/start.sh from MongoDB for tunnel SCP")


class CloudShellConnection:
    """Persistent SSH connection matching Termux's samj6.sh + run_gcloud.sh pattern.

    Uses `script -q -c` to provide a PTY wrapper (like samj6.sh does on Termux),
    and `gcloud cloud-shell ssh` with ServerAliveInterval (like run_gcloud.sh).
    Commands are executed via separate --command sessions.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.connected = False
        self.host = ""
        self.last_activity = time.time()
        self.proc = None
        self.proc_lock = threading.Lock()
        self._monitor_thread = None
        self._tunnel_log = "/tmp/gcloud_tunnel.log"

    def _write_tunnel_script(self) -> str:
        """Write the tunnel script (like run_gcloud.sh) to /tmp."""
        restart_counter = "/tmp/tunnel_restarts"
        start_sh = os.path.join(os.path.dirname(os.path.abspath(__file__)), "start.sh")
        script = """#!/bin/bash
export TERM=xterm
export HOME="${HOME:-/root}"
export PATH="/usr/lib/google-cloud-sdk/bin:$HOME/.local/bin:$PATH"
echo 0 > """ + restart_counter + """
while true; do
    echo "[$(date)] SCP start.sh + starting SSH session..."
    START_TIME=$(date +%s)
    gcloud cloud-shell scp localhost:""" + start_sh + """ cloudshell:~/start.sh 2>>""" + self._tunnel_log + """ && gcloud cloud-shell ssh \\
        --ssh-flag="-o ServerAliveInterval=30" \\
        --ssh-flag="-o ServerAliveCountMax=120" \\
        2>>""" + self._tunnel_log + """
    EXIT_CODE=$?
    END_TIME=$(date +%s)
    DURATION=$(( END_TIME - START_TIME ))
    echo "[$(date)] Session ended (exit=$EXIT_CODE, duration=${DURATION}s). Reconnecting in 5s..."
    if [ $DURATION -lt 60 ]; then
        COUNT=$(cat """ + restart_counter + """ 2>/dev/null || echo 0)
        COUNT=$(( COUNT + 1 ))
        echo $COUNT > """ + restart_counter + """
        echo "[$(date)] SHORT session (${DURATION}s) = possible quota hit. Restart count: $COUNT"
    else
        echo 0 > """ + restart_counter + """
        echo "[$(date)] Long session (${DURATION}s) = normal reset."
    fi
    sleep 5
done
"""
        path = "/tmp/run_tunnel.sh"
        with open(path, "w") as f:
            f.write(script)
        os.chmod(path, 0o755)
        return path

    def connect(self) -> bool:
        with self.proc_lock:
            if self.proc and self.proc.poll() is None:
                _log("[conn] Persistent SSH already connected")
                return True

            tunnel_script = self._write_tunnel_script()

            _log("[conn] Starting persistent SSH via script PTY (samj6.sh pattern)...")
            # Matches Termux samj6.sh: script -q -c "bash $BACKUP_SCRIPT >> log 2>&1"
            cmd = f"script -q -c 'bash {tunnel_script}' >> {self._tunnel_log} 2>&1"
            try:
                self.proc = subprocess.Popen(
                    ["bash", "-c", cmd],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    preexec_fn=os.setsid,
                )
                _log(f"[conn] script PTY started PID={self.proc.pid}")
            except Exception as e:
                _log(f"[conn] Failed to start: {e}")
                return False

        _log("[conn] Waiting 15s for SSH tunnel to establish...")
        time.sleep(15)

        if self.proc.poll() is not None:
            try:
                with open(self._tunnel_log) as f:
                    log_tail = f.read()[-500:]
            except Exception:
                log_tail = "(no log)"
            _log(f"[conn] script PTY died! Log: {log_tail}")
            self.connected = False
            return False

        self.connected = True
        self.last_activity = time.time()
        _log(f"[conn] Persistent SSH via script PTY alive! PID={self.proc.pid}")

        self._start_monitor()
        return True

    def _start_monitor(self):
        def monitor_loop():
            try:
                while self.proc and self.proc.poll() is None:
                    time.sleep(15)
            except Exception:
                pass
            if self.proc:
                exit_code = self.proc.poll()
                try:
                    with open(self._tunnel_log) as f:
                        log_tail = f.read()[-500:]
                except Exception:
                    log_tail = "(no log)"
                _log(f"[conn] SSH tunnel DIED (exit={exit_code}) log_tail={log_tail}")
            self.connected = False

        self._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
        self._monitor_thread.start()

    def _ensure_tunnel(self) -> bool:
        if self.connected and self.proc and self.proc.poll() is None:
            return True
        self.connected = False
        _log("[conn] Tunnel dead, reconnecting...")
        return self._reconnect()

    def _reconnect(self) -> bool:
        self._kill_proc()
        self.connected = False
        time.sleep(2)
        for attempt in range(3):
            _log(f"[conn] Reconnect attempt {attempt+1}/3...")
            if self.connect():
                return True
            time.sleep(5)
        return False

    def _kill_proc(self):
        with self.proc_lock:
            if self.proc:
                try:
                    self.proc.terminate()
                    self.proc.wait(timeout=5)
                except Exception:
                    try:
                        self.proc.kill()
                    except Exception:
                        pass
                self.proc = None

    def disconnect(self):
        self._kill_proc()
        self.connected = False
        self.proc = None
        _log("[conn] Disconnected")

    def _is_alive(self) -> bool:
        return self.connected and self.proc and self.proc.poll() is None

    def _run(self, command: str, timeout: int = None) -> tuple[int, str]:
        if not self._ensure_tunnel():
            return -1, "Connection failed"
        return _run_gcloud_command(command, timeout)


_shell_conn = CloudShellConnection()


def _shell_connect() -> bool:
    return _shell_conn.connect()


def _shell_run(command: str, timeout: int = None) -> tuple[int, str]:
    return _shell_conn._run(command, timeout)


def _tmux_capture_cached() -> str:
    """Return cached tmux capture, refresh in background if stale (>10s)."""
    with _tmux_lock:
        stale = time.time() - _tmux_cache["time"] > 10
        output = _tmux_cache["output"]
    if stale:
        def _refresh():
            rc, out = _shell_run("tmux capture-pane -t main -p -S -10000 2>/dev/null")
            with _tmux_lock:
                _tmux_cache["output"] = out if rc == 0 and out else "[no output]"
                _tmux_cache["time"] = time.time()
        _executor.submit(_refresh)
    return output


def _run_gcloud_command(command: str, timeout: int = None) -> tuple[int, str]:
    cmd = [
        "gcloud", "cloud-shell", "ssh",
        "--authorize-session",
        "--quiet",
        "--command", command,
    ]
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        if timeout:
            out, err = proc.communicate(timeout=timeout)
        else:
            out, err = proc.communicate()
        _shell_conn.last_activity = time.time()
        return proc.returncode, out.strip()
    except subprocess.TimeoutExpired:
        proc.kill()
        return -1, "Timed out"
    except Exception as e:
        return -1, str(e)


def _shell_start_tmux():
    """Check if bot is running on Cloud Shell. .bashrc handles tmux + start.sh + bot."""
    _log("[shell] Checking bot status...")
    check = "tmux has-session -t main 2>/dev/null && echo all_running || echo needs_restart"
    rc, out = _shell_run(check)
    if "all_running" in out:
        _log("[shell] Bot + keepalive already running")
        return True

    _log("[shell] Bot not running yet (.bashrc will start it via SSH)")
    return False


def _shell_capture_tmux():
    """Capture tmux pane output from Cloud Shell (cached)."""
    return _tmux_capture_cached()


def _keepalive_ping() -> bool:
    """Check if persistent SSH connection is alive. ServerAliveInterval handles keepalive."""
    if not _shell_conn._is_alive():
        _log("[keepalive] Persistent SSH not alive, reconnecting...")
        _shell_conn.connected = False
        if not _shell_conn._reconnect():
            return False

    rc, out = _shell_run("echo ping_ok")
    ok = rc == 0 and "ping_ok" in out
    if ok:
        _update_status(last_keepalive=time.time())
    _log(f"[keepalive] rc={rc} ok={ok}")
    return ok


def _get_tunnel_restart_count() -> int:
    """Read tunnel restart counter (written by tunnel script)."""
    try:
        with open("/tmp/tunnel_restarts") as f:
            return int(f.read().strip())
    except Exception:
        return 0


def _is_bot_running() -> bool:
    """Check if bot process is alive on Cloud Shell."""
    rc, out = _shell_run(
        "tmux has-session -t main 2>/dev/null && echo yes || echo no",
    )
    return "yes" in out


def _keepalive_loop():
    _restore_from_mongo()
    _write_start_script()
    _ensure_account_order()
    current_account = _get_current_account()
    if not current_account:
        accounts = mongo_store.get_accounts()
        if accounts:
            current_account = accounts[0]["email"]
            _set_account(current_account)
    mongo_store.set_default_account(current_account)
    _update_status(current_account=current_account, status="connecting", running=True)

    _log("[keepalive] Establishing persistent SSH connection...")
    if not _shell_conn.connect():
        _log("[keepalive] FATAL: Cannot establish persistent SSH")
        _update_status(status="disconnected", running=False)
        return

    _update_status(current_account=current_account, status="connected", running=True)
    fail_count = 0

    while not _keepalive_stop.is_set():
        try:
            if _keepalive_stop.is_set():
                break

            current_account = _get_current_account()
            _update_status(current_account=current_account, status="connected")

            _log(f"=== Keepalive ping | Account: {current_account} ===")
            alive = _keepalive_ping()

            if _keepalive_stop.is_set():
                break

            tunnel_restarts = _get_tunnel_restart_count()
            if tunnel_restarts > 0:
                _log(f"[keepalive] Tunnel restart count: {tunnel_restarts}/{FAIL_THRESHOLD}")

            if alive:
                fail_count = 0
                _update_status(status="connected", last_disconnect=None)

                bot_ok = _is_bot_running()
                if bot_ok:
                    capture = _shell_capture_tmux()
                    if capture and capture != "[no output]":
                        _log(f"[tmux]\n{capture}")
                else:
                    _log("[keepalive] Bot not running yet (.bashrc handling)")
            else:
                _log("[keepalive] Ping failed")
                fail_count = max(fail_count + 1, tunnel_restarts)
                _log(f"[keepalive] Fails: {fail_count}/{FAIL_THRESHOLD}")

            if tunnel_restarts >= FAIL_THRESHOLD:
                _log(f"[keepalive] Tunnel detected {tunnel_restarts} rapid restarts = QUOTA HIT!")
                _update_status(status="disconnected", last_disconnect=time.time())

                next_acc = _get_next_account(current_account)
                if next_acc:
                    _log(f"Quota hit. Switching: {current_account} -> {next_acc}")

                    _log("[keepalive] Killing old bot on Cloud Shell...")
                    _shell_run("tmux kill-session -t main 2>/dev/null; pkill -f cloud_keepalive 2>/dev/null; pkill -f AnonXMusic 2>/dev/null")

                    _set_account(next_acc)
                    mongo_store.set_default_account(next_acc)
                    _update_status(current_account=next_acc)
                    fail_count = 0

                    _log("[keepalive] Killing tunnel to restart with new account...")
                    _shell_conn._reconnect()

                    _save_config_to_mongo()
                    _log("[keepalive] Waiting 10s for new account tunnel...")
                    _keepalive_stop.wait(10)
                else:
                    _log("No other accounts available! Retrying...")
                    fail_count = 0

            _save_config_to_mongo()

            _log(f"Next keepalive in {KEEPALIVE_INTERVAL}s...")
            if alive:
                _update_status(status="connected")
            else:
                _update_status(status="reconnecting")
        except Exception as e:
            _log(f"[keepalive] UNEXPECTED ERROR: {e}")
            _update_status(status="reconnecting")

        _keepalive_stop.wait(KEEPALIVE_INTERVAL)

    _update_status(status="stopped", running=False)
    _log("Keepalive stopped")


def start_keepalive():
    global _keepalive_thread
    if _keepalive_thread and _keepalive_thread.is_alive():
        return False
    _keepalive_stop.clear()
    _keepalive_thread = threading.Thread(target=_keepalive_loop_wrapper, daemon=True)
    _keepalive_thread.start()
    return True


def _keepalive_loop_wrapper():
    """Wrapper that auto-restarts the keepalive loop if it crashes."""
    while not _keepalive_stop.is_set():
        try:
            _keepalive_loop()
            break
        except Exception as e:
            if _keepalive_stop.is_set():
                break
            _log(f"[keepalive] LOOP CRASHED: {e}. Restarting in 10s...")
            _update_status(status="reconnecting")
            _keepalive_stop.wait(10)


def stop_keepalive():
    _keepalive_stop.set()
    return True


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not AUTH_PASSWORD:
            return f(*args, **kwargs)
        if session.get("authed"):
            return f(*args, **kwargs)
        if request.path == "/login" or request.path == "/health":
            return f(*args, **kwargs)
        token = request.args.get("token") or request.headers.get("X-Auth-Token")
        if token and secrets.compare_digest(token, AUTH_PASSWORD):
            session["authed"] = True
            session.permanent = True
            return f(*args, **kwargs)
        return redirect(url_for("login_page"))
    return decorated


LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<title>Login - GCloud Shell</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#0a0e14;color:#c8cdd5;font-family:-apple-system,system-ui,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh}
.login{background:#131720;border:1px solid #1e2530;border-radius:12px;padding:24px;width:100%;max-width:340px;margin:16px}
h1{font-size:18px;color:#fff;text-align:center;margin-bottom:20px}
input{width:100%;padding:10px 14px;border-radius:8px;border:1px solid #1e2530;background:#0a0e14;color:#c8cdd5;font-size:14px;margin-bottom:12px}
input::placeholder{color:#6b7280}
button{width:100%;padding:10px;border-radius:8px;border:none;background:#2563eb;color:#fff;font-size:14px;font-weight:600;cursor:pointer}
button:active{opacity:0.8}
.err{color:#ef4444;font-size:12px;text-align:center;margin-bottom:10px}
</style>
</head>
<body>
<div class="login">
  <h1>GCloud Shell</h1>
  {% if error %}<div class="err">Wrong password</div>{% endif %}
  <form method="POST" action="/login">
    <input type="password" name="password" placeholder="Enter password" autofocus>
    <button type="submit">Login</button>
  </form>
</div>
</body>
</html>"""


@app.route("/login", methods=["GET", "POST"])
def login_page():
    if not AUTH_PASSWORD:
        return redirect("/")
    if request.method == "POST":
        pw = request.form.get("password", "")
        if secrets.compare_digest(pw, AUTH_PASSWORD):
            session["authed"] = True
            return redirect("/")
        return render_template_string(LOGIN_HTML, error=True)
    return render_template_string(LOGIN_HTML, error=False)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/")
@login_required
def index():
    resp = make_response(render_template_string(DASHBOARD_HTML, build_id=BUILD_ID))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/session")
@login_required
def session_info():
    snapshot = {k: v for k, v in _session_info.items()}
    uptime = int(time.time() - snapshot["uptime_start"])
    days, rem = divmod(uptime, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)
    snapshot["uptime_str"] = f"{days}d {hours}h {mins}m {secs}s"
    lk = snapshot.get("last_keepalive")
    if lk:
        ago = int(time.time() - lk)
        m, s = divmod(ago, 60)
        snapshot["last_keepalive_ago"] = f"{m}m {s}s ago"
    else:
        snapshot["last_keepalive_ago"] = "never"
    snapshot["ssh_connected"] = _shell_conn._is_alive()
    snapshot["ssh_host"] = _shell_conn.host or "-"
    return jsonify(snapshot)


@app.route("/log/stream")
@login_required
def log_stream():
    def generate():
        q = queue.Queue(maxsize=200)
        with _lock:
            snapshot = list(_log_buffer)
            _log_subscribers.append(q)
        for line in snapshot:
            yield f"data: {json.dumps({'text': line})}\n\n"
        try:
            while True:
                try:
                    line = q.get(timeout=30)
                    yield f"data: {json.dumps({'text': line})}\n\n"
                except queue.Empty:
                    yield f"data: {json.dumps({'text': ''})}\n\n"
        except GeneratorExit:
            with _lock:
                if q in _log_subscribers:
                    _log_subscribers.remove(q)
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/status/stream")
@login_required
def status_stream():
    def generate():
        q = queue.Queue(maxsize=50)
        with _lock:
            initial = {k: v for k, v in _session_info.items()}
            _status_subscribers.append(q)
        yield f"data: {json.dumps(initial)}\n\n"
        try:
            while True:
                try:
                    snap = q.get(timeout=30)
                    yield f"data: {json.dumps(snap)}\n\n"
                except queue.Empty:
                    yield f"data: {json.dumps({k: v for k, v in _session_info.items()})}\n\n"
        except GeneratorExit:
            with _lock:
                if q in _status_subscribers:
                    _status_subscribers.remove(q)
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/accounts")
@login_required
def list_accounts():
    return jsonify({"accounts": mongo_store.get_accounts(), "default": mongo_store.get_default_account()})


@app.route("/accounts", methods=["POST"])
@login_required
def add_account():
    data = request.get_json(force=True)
    email = data.get("email", "")
    if not email:
        return jsonify({"error": "email required"}), 400
    mongo_store.add_account(email, data.get("password", ""), data.get("default", False), data.get("sort_order", 99))
    _log(f"Added account: {email}")
    return jsonify({"status": "added", "email": email})


@app.route("/accounts/<email>", methods=["DELETE"])
@login_required
def remove_account(email):
    mongo_store.remove_account(email)
    _log(f"Removed account: {email}")
    return jsonify({"status": "removed", "email": email})


@app.route("/accounts/switch/<email>", methods=["POST"])
@login_required
def switch_account(email):
    mongo_store.set_default_account(email)
    _update_status(current_account=email)
    _log(f"[switch] Dashboard switching to {email}...")
    threading.Thread(target=_dashboard_switch_reconnect, args=(email,), daemon=True).start()
    return jsonify({"status": "switched", "email": email})


def _dashboard_switch_reconnect(email: str):
    """Kill everything old, then reconnect with new account."""
    try:
        _log(f"[switch] Killing old bot on Cloud Shell...")
        _shell_run("tmux kill-session -t main 2>/dev/null; pkill -f cloud_keepalive 2>/dev/null; pkill -f AnonXMusic 2>/dev/null")

        _log(f"[switch] Killing old gcloud processes on Render...")
        subprocess.run(["pkill", "-f", "gcloud cloud-shell"], timeout=5)
        subprocess.run(["pkill", "-f", "gcloud-container"], timeout=5)
        time.sleep(2)

        _set_account(email)
        _save_config_to_mongo()
        _shell_conn._reconnect()
        _log(f"[switch] Reconnected with {email}")
    except Exception as e:
        _log(f"[switch] Reconnect failed: {e}")


@app.route("/keepalive/start", methods=["POST"])
@login_required
def api_start():
    _log("[start] Starting keepalive loop...")
    ok = start_keepalive()
    return jsonify({"status": "started" if ok else "already running"})


@app.route("/keepalive/stop", methods=["POST"])
@login_required
def api_stop():
    _log("[stop] Stopping keepalive loop only...")
    stop_keepalive()
    _update_status(status="stopped", running=False)
    return jsonify({"status": "stopped"})


@app.route("/kill-bot", methods=["POST"])
@login_required
def kill_bot():
    _log("[kill-bot] Killing bot processes on Cloud Shell...")

    def _run():
        rc, out = _shell_run(
            "tmux kill-session -t main 2>/dev/null; "
            "pkill -f cloud_keepalive 2>/dev/null; "
            "pkill -f AnonXMusic 2>/dev/null; "
            "echo killed",
        )
        _log(f"[kill-bot] rc={rc} out={out}")

    _executor.submit(_run)
    return jsonify({"status": "killing"})


@app.route("/exec", methods=["POST"])
@login_required
def exec_command():
    data = request.get_json(force=True)
    command = data.get("command", "")
    if not command:
        return jsonify({"error": "No command"}), 400
    _log(f"[exec] Running: {command}")
    _update_status(last_command=command, exec_running=True, last_exec_output=None, last_exec_rc=None)

    def _run():
        rc, out = _shell_run(command, timeout=120)
        _update_status(last_exec_output=out, last_exec_rc=rc, exec_running=False)
        _log(f"[exec] Done: rc={rc}")

    _executor.submit(_run)
    return jsonify({"status": "running"})


@app.route("/exec/result")
@login_required
def exec_result():
    return jsonify({
        "output": _session_info.get("last_exec_output"),
        "returncode": _session_info.get("last_exec_rc"),
        "running": _session_info.get("exec_running", False),
    })


@app.route("/config/save", methods=["POST"])
@login_required
def config_save():
    _save_config_to_mongo()
    return jsonify({"status": "saved"})


@app.route("/config/restore", methods=["POST"])
@login_required
def config_restore():
    _restore_from_mongo()
    return jsonify({"status": "restored"})


@app.route("/startscript", methods=["GET"])
@login_required
def get_startscript():
    content = mongo_store.get_start_script() or ""
    return jsonify({"content": content})


@app.route("/startscript", methods=["POST"])
@login_required
def save_startscript():
    data = request.get_json(force=True)
    content = data.get("content", "")
    if not content:
        return jsonify({"error": "content required"}), 400
    mongo_store.save_start_script(content)
    _write_start_script()
    _log("start.sh saved to MongoDB and written to /app/start.sh")
    return jsonify({"status": "saved"})


@app.route("/tmux/start", methods=["POST"])
@login_required
def tmux_start():
    ok = _shell_start_tmux()
    return jsonify({"status": "started" if ok else "failed"})


@app.route("/tmux/capture")
@login_required
def tmux_capture():
    out = _shell_capture_tmux()
    return jsonify({"output": out})


@app.route("/tmux/send", methods=["POST"])
@login_required
def tmux_send():
    data = request.get_json(force=True)
    keys = data.get("keys", "")
    if not keys:
        return jsonify({"error": "keys required"}), 400
    _executor.submit(_shell_run, f"tmux send-keys -t main '{keys}' Enter")
    return jsonify({"status": "sent"})


class RenderShellPTY:
    """Persistent PTY session for Render container shell."""

    def __init__(self):
        self.pid = None
        self.master_fd = None
        self.output_buffer = deque(maxlen=100000)
        self.subscribers: list[queue.Queue] = []
        self.lock = threading.Lock()
        self.alive = False
        self.reader_thread = None
        self.seq = 0
        self.buffer_start_seq = 0
        self.cols = 80
        self.rows = 24

    def start(self, cols=80, rows=24):
        if self.alive:
            return
        with self.lock:
            self.output_buffer.clear()
            self.seq = 0
            self.buffer_start_seq = 0
        self.cols = cols
        self.rows = rows
        self.pid, self.master_fd = pty.fork()
        if self.pid == 0:
            os.environ["TERM"] = "xterm-256color"
            os.environ["COLUMNS"] = str(cols)
            os.environ["LINES"] = str(rows)
            os.execvp("/bin/bash", ["/bin/bash", "-l"])
        os.set_blocking(self.master_fd, False)
        fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
        self.alive = True
        self.reader_thread = threading.Thread(target=self._reader, daemon=True)
        self.reader_thread.start()
        _log("[render-shell] PTY started")

    def _reader(self):
        while self.alive and self.master_fd is not None:
            try:
                r, _, _ = select.select([self.master_fd], [], [], 0.1)
                if r:
                    data = os.read(self.master_fd, 4096)
                    if not data:
                        break
                    text = data.decode("utf-8", errors="replace")
                    with self.lock:
                        if len(self.output_buffer) == self.output_buffer.maxlen:
                            self.buffer_start_seq += len(self.output_buffer[0])
                        self.output_buffer.append(text)
                        self.seq += len(text)
                        dead = []
                        for q in self.subscribers:
                            try:
                                q.put_nowait(text)
                            except queue.Full:
                                dead.append(q)
                        for q in dead:
                            self.subscribers.remove(q)
            except (OSError, TypeError):
                break
        self.alive = False
        _log("[render-shell] PTY reader stopped")

    def write(self, data: str):
        if self.alive and self.master_fd is not None:
            try:
                os.write(self.master_fd, data.encode())
                return True
            except OSError:
                pass
        return False

    def resize(self, cols: int, rows: int):
        if self.alive and self.master_fd is not None:
            try:
                fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))
                self.cols = cols
                self.rows = rows
                try:
                    os.kill(self.pid, 28)  # SIGWINCH
                except Exception:
                    pass
                return True
            except OSError:
                pass
        return False

    def stop(self):
        self.alive = False
        if self.master_fd is not None:
            try:
                os.close(self.master_fd)
            except Exception:
                pass
            self.master_fd = None
        if self.pid:
            try:
                os.kill(self.pid, 9)
            except Exception:
                pass
            self.pid = None
        _log("[render-shell] PTY stopped")

    def get_output(self) -> str:
        with self.lock:
            return "".join(self.output_buffer)


_render_shell_pty = RenderShellPTY()

BUILD_ID = str(int(time.time()))


@app.route("/render-shell")
@login_required
def render_shell_page():
    if request.args.get("v") != BUILD_ID:
        return redirect(f"/render-shell?v={BUILD_ID}")
    if not _render_shell_pty.alive:
        _render_shell_pty.start()
    resp = make_response(render_template_string(RENDER_SHELL_HTML))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/render-shell/output")
@login_required
def render_shell_output():
    try:
        last_id = int(request.headers.get("Last-Event-ID", "") or "0")
    except ValueError:
        last_id = 0

    def generate():
        q = queue.Queue(maxsize=8000)
        with _render_shell_pty.lock:
            _render_shell_pty.subscribers.append(q)
            buf_text = "".join(_render_shell_pty.output_buffer)
            buf_start = _render_shell_pty.buffer_start_seq
            seq_now = _render_shell_pty.seq

        if last_id <= buf_start and buf_text:
            send = buf_text[-100000:]
            start_pos = seq_now - len(send)
        elif last_id < seq_now and buf_text:
            offset = max(0, last_id - buf_start)
            send = buf_text[offset:]
            start_pos = last_id
        else:
            send = ""
            start_pos = seq_now

        if send:
            yield f"id: {start_pos + len(send)}\ndata: {json.dumps({'text': send})}\n\n"

        pos = start_pos + len(send)
        try:
            while _render_shell_pty.alive:
                try:
                    chunk = q.get(timeout=15)
                except queue.Empty:
                    yield ": ping\n\n"
                    continue
                pos += len(chunk)
                yield f"id: {pos}\ndata: {json.dumps({'text': chunk})}\n\n"
        except GeneratorExit:
            pass
        finally:
            with _render_shell_pty.lock:
                if q in _render_shell_pty.subscribers:
                    _render_shell_pty.subscribers.remove(q)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/render-shell/input", methods=["POST"])
@login_required
def render_shell_input():
    if not _render_shell_pty.alive:
        _render_shell_pty.start()
    data = request.get_json(force=True)
    keys = data.get("keys", "")
    cols = data.get("cols", 0)
    rows = data.get("rows", 0)
    if data.get("resize") or (cols and rows and (cols != _render_shell_pty.cols or rows != _render_shell_pty.rows)):
        _render_shell_pty.resize(cols, rows)
        if data.get("resize"):
            return jsonify({"status": "resized"})
    if keys:
        _render_shell_pty.write(keys)
    return jsonify({"status": "ok"})


@app.route("/render-shell/restart", methods=["POST"])
@login_required
def render_shell_restart():
    _render_shell_pty.stop()
    time.sleep(0.5)
    _render_shell_pty.start()
    return jsonify({"status": "restarted"})


RENDER_SHELL_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover, interactive-widget=resizes-content">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<title>Render Shell</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.min.css">
<style>
*{margin:0;padding:0;box-sizing:border-box}
html{height:100%}
body{background:#0a0e14;color:#c8cdd5;font-family:-apple-system,system-ui,sans-serif;overflow:hidden;height:100%;height:100dvh;display:flex;flex-direction:column;-webkit-tap-highlight-color:transparent}
.header{display:flex;align-items:center;gap:8px;padding:7px 10px;border-bottom:1px solid #1e2530;background:#131720;flex-shrink:0;padding-top:calc(7px + env(safe-area-inset-top))}
.header h1{font-size:13px;font-weight:700;color:#fff;white-space:nowrap}
.header a{color:#3b82f6;font-size:11px;text-decoration:none;white-space:nowrap}
.status{font-size:10px;padding:2px 7px;border-radius:6px;white-space:nowrap}
.status.on{background:#052e16;color:#22c55e}
.status.off{background:#450a0a;color:#ef4444}
#termWrap{flex:1;min-height:0;position:relative;overflow:hidden}
#term{position:absolute;inset:0;padding:3px 2px}
.xterm-viewport::-webkit-scrollbar{width:4px}
.xterm-viewport::-webkit-scrollbar-thumb{background:#1e2530;border-radius:2px}
.xterm-helper-textarea{caret-color:transparent!important;color:transparent!important;background:transparent!important}
.xterm-helper-textarea::selection{background:transparent}

.keypad{flex-shrink:0;background:#131720;border-top:1px solid #1e2530;padding:4px 4px;padding-bottom:calc(4px + env(safe-area-inset-bottom))}
.krow{display:flex;gap:4px}
.krow+.krow{margin-top:4px}
.kb{flex:1 1 0;min-width:0;height:36px;padding:0 1px;background:#1a2030;border:1px solid #2a3345;border-radius:6px;color:#c8cdd5;font-size:12px;font-family:ui-monospace,Menlo,monospace;cursor:pointer;user-select:none;-webkit-user-select:none;touch-action:manipulation;display:flex;align-items:center;justify-content:center;white-space:nowrap;overflow:hidden}
.kb:active{background:#2563eb;border-color:#2563eb;color:#fff}
.kb.armed{background:#2563eb;border-color:#60a5fa;color:#fff;box-shadow:0 0 0 2px #2563eb55}
</style>
</head>
<body>
<div class="header">
  <h1>Render Shell</h1>
  <span class="status off" id="connStatus">Connecting...</span>
  <a href="/" style="margin-left:auto">&larr; Dashboard</a>
</div>
<div id="termWrap"><div id="term"></div></div>
<div class="keypad" id="extraKeys">
  <div class="krow">
    <button class="kb" data-k="esc">esc</button>
    <button class="kb" data-mod="ctrl">ctrl</button>
    <button class="kb" data-mod="alt">alt</button>
    <button class="kb" data-k="tab">tab</button>
    <button class="kb" data-k="left">&larr;</button>
    <button class="kb" data-k="up">&uarr;</button>
    <button class="kb" data-k="down">&darr;</button>
    <button class="kb" data-k="right">&rarr;</button>
    <button class="kb" data-k="home">home</button>
    <button class="kb" data-k="end">end</button>
  </div>
  <div class="krow">
    <button class="kb" data-k="pgup">pgup</button>
    <button class="kb" data-k="pgdn">pgdn</button>
    <button class="kb" data-k="bksp">&#9003;</button>
    <button class="kb" data-k="pipe">|</button>
    <button class="kb" data-k="dash">-</button>
    <button class="kb" data-k="slash">/</button>
    <button class="kb" data-k="tilde">~</button>
    <button class="kb" data-k="dollar">$</button>
    <button class="kb" data-k="space">spc</button>
    <button class="kb" data-k="enter">enter</button>
  </div>
</div>
<script src="https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xterm-addon-fit@0.8.0/lib/xterm-addon-fit.min.js"></script>
<script>
const status=document.getElementById('connStatus');
const wrap=document.getElementById('termWrap');

const term=new Terminal({
  theme:{background:'#0a0e14',foreground:'#c8cdd5',cursor:'#22c55e',cursorAccent:'#000',selectionBackground:'#264f78',
    black:'#1c1c1c',red:'#ff5555',green:'#50fa7b',yellow:'#f1fa8c',blue:'#bd93f9',magenta:'#ff79c6',cyan:'#8be9fd',white:'#d8d8d8',
    brightBlack:'#6272a4',brightRed:'#ff6e6e',brightGreen:'#69ff94',brightYellow:'#ffffa5',brightMagenta:'#ff92df',brightCyan:'#a4ffff',brightWhite:'#ffffff'},
  fontFamily:'ui-monospace,SFMono-Regular,Menlo,Consolas,monospace',
  fontSize:14,cursorBlink:true,scrollback:10000,
  allowProposedApi:true,convertEol:false,
  macOptionIsMeta:true,scrollOnUserInput:true,
  minimumContrastRatio:0
});
const fitAddon=new FitAddon.FitAddon();
term.loadAddon(fitAddon);

let resizeTimer=null;
term.onResize(({cols,rows})=>{
  clearTimeout(resizeTimer);
  resizeTimer=setTimeout(()=>{
    fetch('/render-shell/input',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({resize:true,cols,rows})}).catch(()=>{});
  },80);
});

term.open(document.getElementById('term'));
fitAddon.fit();

function safeFit(){
  try{fitAddon.fit()}catch(e){}
  term.focus();
}
function viewportFix(){
  const vv=window.visualViewport;
  if(vv){ document.body.style.height=vv.height+'px'; }
  setTimeout(safeFit,80);
}
window.addEventListener('resize',viewportFix);
window.addEventListener('orientationchange',()=>setTimeout(viewportFix,300));
if(window.visualViewport){
  window.visualViewport.addEventListener('resize',viewportFix);
  window.visualViewport.addEventListener('scroll',viewportFix);
}

const KEYMAP={
  esc:'\x1b', tab:'\t',
  left:'\x1b[D', up:'\x1b[A', down:'\x1b[B', right:'\x1b[C',
  home:'\x1b[H', end:'\x1b[F', pgup:'\x1b[5~', pgdn:'\x1b[6~',
  bksp:'\x7f', enter:'\r', space:' ',
  pipe:'|', dash:'-', slash:'/', tilde:'~', dollar:'$'
};

let ctrlArmed=false, altArmed=false;

function ctrlSeq(d){
  if(d.length===1){
    const c=d.charCodeAt(0);
    if(d>='a'&&d<='z') return String.fromCharCode(c-96);
    if(d>='A'&&d<='Z') return String.fromCharCode(c-64);
    if(d===' ') return '\x00';
    if(d==='@') return '\x00';
    if(d==='[') return '\x1b';
    if(d==='\\') return '\x1c';
    if(d===']') return '\x1d';
    if(d==='^') return '\x1e';
    if(d==='_') return '\x1f';
    if(d==='?') return '\x7f';
    if(c>=96&&c<=127) return String.fromCharCode(c-96);
  }
  return d;
}

function buildInput(raw, isChar){
  let d=raw;
  const csi=d.startsWith('\x1b[');
  if(ctrlArmed && isChar) d=ctrlSeq(d);
  else if(ctrlArmed && csi){
    d=d.replace(/\[([A-Z0-9~])$/,'[1;5$1');
  }
  if(altArmed){
    if(csi && !ctrlArmed) d=d.replace(/\[([A-Z0-9~])$/,'[1;3$1');
    else if(!(d.startsWith('\x1b')&&d.length>2)) d='\x1b'+d;
  }
  return d;
}

let outBuf='', outTimer=null;
function sendRaw(d){
  outBuf+=d;
  if(outTimer) return;
  outTimer=setTimeout(()=>{
    const keys=outBuf; outBuf=''; outTimer=null;
    fetch('/render-shell/input',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({keys,cols:term.cols,rows:term.rows})}).catch(()=>{});
  },25);
}

term.onData(d=>{
  if(/^\x1b\[\d+;\d+R$/.test(d)){ sendRaw(d); return; }
  const isChar=d.length===1&&!d.startsWith('\x1b');
  sendRaw(buildInput(d,isChar));
  try{
    if(term.buffer.active.viewportY<term.buffer.active.baseY-1) term.scrollToBottom();
  }catch(e){}
});

const extraKeys=document.getElementById('extraKeys');
extraKeys.addEventListener('pointerdown',e=>{
  const b=e.target.closest('.kb');
  if(!b) return;
  e.preventDefault();
});
extraKeys.addEventListener('click',e=>{
  const b=e.target.closest('.kb');
  if(!b) return;
  if(b.dataset.mod){
    if(b.dataset.mod==='ctrl'){ctrlArmed=!ctrlArmed;b.classList.toggle('armed',ctrlArmed)}
    else{altArmed=!altArmed;b.classList.toggle('armed',altArmed)}
    term.focus();
    return;
  }
  const seq=KEYMAP[b.dataset.k];
  if(seq!==undefined){
    const isChar=seq.length===1&&!seq.startsWith('\x1b');
    sendRaw(buildInput(seq,isChar));
    term.focus();
  }
});

wrap.addEventListener('click',()=>term.focus());

let pendingText='', writeRaf=null;
function queueWrite(t){
  pendingText+=t;
  if(writeRaf) return;
  writeRaf=requestAnimationFrame(()=>{
    const txt=pendingText; pendingText=''; writeRaf=null;
    const atBottom=term.buffer.active.viewportY>=term.buffer.active.baseY-2;
    term.write(txt,()=>{
      if(atBottom) term.scrollToBottom();
    });
  });
}

let es=null;
function connectSSE(){
  es=new EventSource('/render-shell/output');
  es.onopen=()=>{status.textContent='Connected';status.className='status on'};
  es.onerror=()=>{status.textContent='Reconnecting...';status.className='status off'};
  es.onmessage=e=>{
    try{
      const d=JSON.parse(e.data);
      if(d.text) queueWrite(d.text);
    }catch(ex){}
  };
}
connectSSE();

document.addEventListener('visibilitychange',()=>{
  if(!document.hidden){ safeFit(); }
});
term.focus();
</script>
</body>
</html>"""


DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="mobile-web-app-capable" content="yes">
<title>GCloud Shell</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/xterm@5.3.0/css/xterm.min.css">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0e14;--card:#131720;--border:#1e2530;--text:#c8cdd5;--dim:#6b7280;--green:#22c55e;--red:#ef4444;--yellow:#eab308;--blue:#3b82f6;--accent:#2563eb}
html,body{background:var(--bg);color:var(--text);font-family:-apple-system,system-ui,sans-serif;font-size:14px;-webkit-text-size-adjust:100%;overscroll-behavior:none}
a{color:var(--blue)}

.header{display:flex;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid var(--border)}
.header h1{font-size:16px;font-weight:700;color:#fff;white-space:nowrap}
.badge{display:inline-flex;align-items:center;gap:5px;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.badge::before{content:'';width:6px;height:6px;border-radius:50%}
.badge.connected{background:#052e16;color:var(--green)}.badge.connected::before{background:var(--green)}
.badge.reconnecting{background:#422006;color:var(--yellow)}.badge.reconnecting::before{background:var(--yellow)}
.badge.disconnected{background:#450a0a;color:var(--red)}.badge.disconnected::before{background:var(--red)}
.badge.stopped{background:#1e2530;color:var(--dim)}.badge.stopped::before{background:var(--dim)}
.badge.initializing{background:#1e2530;color:var(--dim)}.badge.initializing::before{background:var(--dim)}

.content{padding:12px 16px;display:flex;flex-direction:column;gap:12px}

.section{background:var(--card);border:1px solid var(--border);border-radius:10px;overflow:hidden}
.section-head{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;border-bottom:1px solid var(--border)}
.section-head h2{font-size:12px;color:var(--dim);text-transform:uppercase;letter-spacing:0.5px}
.section-body{padding:12px}

.row{display:flex;justify-content:space-between;padding:6px 0;font-size:13px}
.row .k{color:var(--dim)}.row .v{font-weight:600;text-align:right;word-break:break-all}

.btns{display:flex;gap:8px;flex-wrap:wrap}
.btn{display:inline-flex;align-items:center;justify-content:center;padding:8px 16px;border-radius:8px;border:none;font-size:13px;font-weight:600;cursor:pointer;transition:opacity 0.15s}
.btn:active{opacity:0.7}
.btn.green{background:var(--green);color:#000}
.btn.red{background:var(--red);color:#fff}
.btn.blue{background:var(--blue);color:#fff}
.btn.gray{background:#374151;color:#fff}
.btn:disabled{opacity:0.4;cursor:default}

.acct-list{display:flex;flex-direction:column;gap:6px}
.acct{display:flex;align-items:center;gap:8px;padding:8px 10px;border-radius:8px;font-size:12px;border:1px solid var(--border);transition:border-color 0.15s}
.acct.active{border-color:var(--green);background:#052e16}
.acct .email{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.acct .tag{font-size:9px;padding:2px 6px;border-radius:6px;background:var(--green);color:#000;font-weight:700}
.acct .btns{gap:4px}
.btn.sm{padding:4px 10px;font-size:11px;border-radius:6px}

.add-row{display:flex;gap:6px;margin-top:8px}
.add-row input{flex:1;padding:8px 10px;border-radius:8px;border:1px solid var(--border);background:var(--bg);color:var(--text);font-size:13px}
.add-row input::placeholder{color:var(--dim)}

#log-term{height:280px;background:#000;border-radius:6px}
.xterm-viewport::-webkit-scrollbar{width:4px}
.xterm-viewport::-webkit-scrollbar-thumb{background:var(--border);border-radius:2px}

.exec-input{display:flex;gap:6px}
.exec-input input{flex:1;padding:8px 10px;border-radius:8px;border:1px solid var(--border);background:var(--bg);color:var(--text);font-size:13px;font-family:monospace}
.exec-out{margin-top:8px;padding:10px;background:#000;border-radius:6px;font-family:monospace;font-size:12px;color:var(--green);max-height:180px;overflow-y:auto;white-space:pre-wrap;word-break:break-all;display:none}

.toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);padding:8px 16px;border-radius:8px;font-size:13px;font-weight:600;z-index:9999;pointer-events:none;animation:tIn 0.2s}
.toast.ok{background:var(--green);color:#000}
.toast.err{background:var(--red);color:#fff}
@keyframes tIn{from{opacity:0;transform:translateX(-50%) translateY(8px)}to{opacity:1;transform:translateX(-50%) translateY(0)}}
</style>
</head>
<body>
<div class="header">
  <h1>GCloud Shell</h1>
  <span class="badge initializing" id="badge">initializing</span>
  <span style="flex:1"></span>
  <a href="/render-shell?v={{build_id}}" style="font-size:12px;color:var(--blue);text-decoration:none;padding:4px 8px;border:1px solid var(--border);border-radius:6px">Render Shell</a>
  <span id="uptime" style="font-size:11px;color:var(--dim)"></span>
</div>
<div class="content">

  <div class="section">
    <div class="section-head">
      <h2>GCloud Shell</h2>
      <div class="btns" id="ctrlBtns">
        <button class="btn green" onclick="startShell()">Start</button>
        <button class="btn red" onclick="shutdownShell()">Shutdown</button>
        <button class="btn gray" onclick="killBot()">Kill Bot</button>
      </div>
    </div>
    <div class="section-body">
      <div class="row"><span class="k">Status</span><span class="v" id="sStatus">-</span></div>
      <div class="row"><span class="k">Account</span><span class="v" id="sAccount">-</span></div>
      <div class="row"><span class="k">Fails</span><span class="v" id="sFails">0</span></div>
      <div class="row"><span class="k">Last Keepalive</span><span class="v" id="sKeepalive">-</span></div>
      <div class="row"><span class="k">Last Command</span><span class="v" id="sLastCmd">-</span></div>
      <div class="btns" style="margin-top:10px">
        <button class="btn gray" onclick="saveCfg()">Save Config</button>
        <button class="btn gray" onclick="restoreCfg()">Restore Config</button>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-head"><h2>Accounts</h2></div>
    <div class="section-body">
      <div class="acct-list" id="acctList">Loading...</div>
      <div class="add-row">
        <input type="email" id="newEmail" placeholder="email@gmail.com" onkeydown="if(event.key==='Enter')addAcct()">
        <button class="btn blue" onclick="addAcct()">Add</button>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-head"><h2>Live Log</h2></div>
    <div class="section-body" style="padding:8px">
      <div id="log-term"></div>
    </div>
  </div>

  <div class="section">
    <div class="section-head"><h2>start.sh (Cloud Shell)</h2></div>
    <div class="section-body">
      <div style="font-size:12px;color:var(--dim);margin-bottom:8px">Stored in MongoDB. SCP'd to Cloud Shell before SSH.</div>
      <textarea id="startShContent" style="width:100%;height:120px;background:var(--bg);color:var(--green);border:1px solid var(--border);border-radius:8px;padding:10px;font-family:monospace;font-size:11px;resize:vertical" placeholder="#!/bin/bash&#10;export TZ=Asia/Kolkata&#10;..."></textarea>
      <div class="btns" style="margin-top:8px">
        <button class="btn gray" onclick="loadStartSh()">Load</button>
        <button class="btn blue" onclick="saveStartSh()">Save</button>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-head">
      <h2>Cloud Shell Terminal</h2>
      <div class="btns">
        <button class="btn gray sm" onclick="tmuxStart()">Start Bot</button>
        <button class="btn gray sm" onclick="tmuxRefresh()">Refresh</button>
      </div>
    </div>
    <div class="section-body" style="padding:8px">
      <pre id="tmuxOut" style="background:#000;color:var(--green);padding:10px;border-radius:6px;font-size:11px;max-height:300px;overflow:auto;margin:0;white-space:pre-wrap;word-break:break-all;min-height:80px">Loading...</pre>
      <div class="exec-input" style="margin-top:8px">
        <input type="text" id="tmuxInput" placeholder="Type command and press Enter..." onkeydown="if(event.key==='Enter')tmuxSend()">
        <button class="btn blue" onclick="tmuxSend()">Send</button>
      </div>
    </div>
  </div>

  <div class="section">
    <div class="section-head"><h2>Run Command</h2></div>
    <div class="section-body">
      <div class="exec-input">
        <input type="text" id="execCmd" placeholder="gcloud auth list" onkeydown="if(event.key==='Enter')runCmd()">
        <button class="btn blue" onclick="runCmd()">Run</button>
      </div>
      <div class="exec-out" id="execOut"></div>
    </div>
  </div>

</div>

<script src="https://cdn.jsdelivr.net/npm/xterm@5.3.0/lib/xterm.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/xterm-addon-fit@0.8.0/lib/xterm-addon-fit.min.js"></script>
<script>
const term=new Terminal({theme:{background:'#000',foreground:'#c8cdd5',cursor:'#c8cdd5',cursorAccent:'#000',selectionBackground:'#264f78'},fontFamily:"monospace",fontSize:12,cursorBlink:false,scrollback:3000});
const fitAddon=new FitAddon.FitAddon();
term.loadAddon(fitAddon);
term.open(document.getElementById('log-term'));
new ResizeObserver(()=>fitAddon.fit()).observe(document.getElementById('log-term'));

function toast(m,t='ok'){const d=document.createElement('div');d.className='toast '+t;d.textContent=m;document.body.appendChild(d);setTimeout(()=>d.remove(),2500)}

async function getJSON(url,opts){const r=await fetch(url,opts);return r.json()}

function applyStatus(d){
  document.getElementById('sStatus').textContent=d.status||'-';
  document.getElementById('sAccount').textContent=d.current_account||'-';
  document.getElementById('sFails').textContent=d.fail_count||0;
  document.getElementById('sKeepalive').textContent=d.last_keepalive_ago||'-';
  document.getElementById('sLastCmd').textContent=d.last_command||'-';
  document.getElementById('uptime').textContent=d.uptime_str||'';
  const b=document.getElementById('badge');
  b.textContent=d.status||'unknown';
  b.className='badge '+(d.status||'initializing');
}

async function loadAccts(){
  try{
    const d=await getJSON('/accounts');
    const el=document.getElementById('acctList');
    if(!d.accounts||!d.accounts.length){el.innerHTML='<div style="color:var(--dim);text-align:center;padding:16px">No accounts</div>';return}
    el.innerHTML=d.accounts.map(a=>'<div class="acct '+(a.email===d.default?'active':'')+'">'+
      '<span class="email">'+a.email+'</span>'+
      (a.email===d.default?'<span class="tag">ACTIVE</span>':'')+
      '<div class="btns">'+
      (a.email!==d.default?'<button class="btn sm blue" onclick="switchAcct(\''+a.email+'\')">Switch</button>':'')+
      '<button class="btn sm red" onclick="rmAcct(\''+a.email+'\')">X</button>'+
      '</div></div>').join('');
  }catch(e){}
}

async function addAcct(){
  const e=document.getElementById('newEmail').value.trim();if(!e)return;
  try{await getJSON('/accounts',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:e})});
  document.getElementById('newEmail').value='';toast('Added');loadAccts()}catch(e){toast('Failed','err')}
}

async function rmAcct(e){
  if(!confirm('Remove '+e+'?'))return;
  try{await getJSON('/accounts/'+encodeURIComponent(e),{method:'DELETE'});toast('Removed');loadAccts()}catch(e){toast('Failed','err')}
}

async function switchAcct(e){
  try{await getJSON('/accounts/switch/'+encodeURIComponent(e),{method:'POST'});toast('Switched');loadAccts();fetch('/session').then(r=>r.json()).then(applyStatus).catch(()=>{})}catch(e){toast('Failed','err')}
}

async function startShell(){
  try{await getJSON('/keepalive/start',{method:'POST'});toast('Cloud Shell starting...');setTimeout(()=>fetch('/session').then(r=>r.json()).then(applyStatus).catch(()=>{}),2000)}catch(e){toast('Failed','err')}
}

async function shutdownShell(){
  try{await getJSON('/keepalive/stop',{method:'POST'});toast('Shutting down...');setTimeout(()=>fetch('/session').then(r=>r.json()).then(applyStatus).catch(()=>{}),2000)}catch(e){toast('Failed','err')}
}

async function killBot(){
  try{await getJSON('/kill-bot',{method:'POST'});toast('Bot killed');setTimeout(()=>fetch('/session').then(r=>r.json()).then(applyStatus).catch(()=>{}),2000)}catch(e){toast('Failed','err')}
}

async function saveCfg(){
  try{await getJSON('/config/save',{method:'POST'});toast('Saved')}catch(e){toast('Failed','err')}
}

async function restoreCfg(){
  try{await getJSON('/config/restore',{method:'POST'});toast('Restored');fetch('/session').then(r=>r.json()).then(applyStatus).catch(()=>{})}catch(e){toast('Failed','err')}
}

async function runCmd(){
  const c=document.getElementById('execCmd').value.trim();if(!c)return;
  const o=document.getElementById('execOut');o.style.display='block';o.textContent='Running...';o.style.color='var(--fg)';
  try{
    await getJSON('/exec',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({command:c})});
    let tries=0;
    const poll=async()=>{
      const r=await getJSON('/exec/result');
      if(!r.running||tries>120){
        o.textContent=r.output||'(no)';o.style.color=r.returncode===0?'var(--green)':'var(--red)';
      }else{tries++;o.textContent='Running... ('+tries+'s)';setTimeout(poll,1000)}
    };
    await poll();
  }catch(e){o.textContent='Error: '+e;o.style.color='var(--red)'}
}

async function tmuxStart(){
  try{const d=await getJSON('/tmux/start',{method:'POST'});toast(d.status==='started'?'tmux started':'failed');tmuxRefresh()}catch(e){toast('Failed','err')}
}

async function tmuxRefresh(){
  try{const d=await getJSON('/tmux/capture');document.getElementById('tmuxOut').textContent=d.output||'(empty)';document.getElementById('tmuxOut').scrollTop=99999}catch(e){document.getElementById('tmuxOut').textContent='Error: '+e}
}

async function tmuxSend(){
  const k=document.getElementById('tmuxInput').value.trim();if(!k)return;
  try{await getJSON('/tmux/send',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({keys:k})});document.getElementById('tmuxInput').value='';setTimeout(tmuxRefresh,1000)}catch(e){toast('Failed','err')}
}

async function loadStartSh(){
  try{const d=await getJSON('/startscript');document.getElementById('startShContent').value=d.content||'';toast('Loaded')}catch(e){toast('Failed','err')}
}
async function saveStartSh(){
  const c=document.getElementById('startShContent').value.trim();if(!c)return;
  try{await getJSON('/startscript',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({content:c})});toast('Saved')}catch(e){toast('Failed','err')}
}

setInterval(tmuxRefresh,60000);

let _scrollTimer=null;
const es=new EventSource('/log/stream');
es.onmessage=e=>{try{const d=JSON.parse(e.data);if(d.text){term.writeln(d.text);if(_scrollTimer)clearTimeout(_scrollTimer);_scrollTimer=setTimeout(()=>{term.scrollToBottom();_scrollTimer=null},50)}}catch(ex){}};
es.onerror=()=>{setTimeout(()=>{try{es.close()}catch(e){};setTimeout(()=>location.reload(),5000)},3000)};

const sse=new EventSource('/status/stream');
sse.onmessage=e=>{try{applyStatus(JSON.parse(e.data))}catch(ex){}};
sse.onerror=()=>{setTimeout(()=>{try{sse.close()}catch(e){};setTimeout(()=>location.reload(),5000)},5000)};

loadAccts();loadStartSh();
setInterval(loadAccts,30000);
</script>
</body>
</html>"""


def main():
    _log("=== GCloud Cloud Shell - Render Active Mode ===")
    _log("Auto-starting keepalive on boot...")
    start_keepalive()
    app.run(host="0.0.0.0", port=PORT, debug=False, threaded=True)


if __name__ == "__main__":
    main()
