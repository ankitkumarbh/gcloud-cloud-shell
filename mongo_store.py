import os
import json
import base64
import shutil
import logging
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "")
DB_NAME = os.environ.get("MONGO_DB", "gcloud_shell")

_client = None
_db = None


def _get_db():
    global _client, _db
    if _db is None:
        _client = MongoClient(MONGO_URI)
        _db = _client[DB_NAME]
        try:
            _db["accounts"].create_index("email", unique=True)
        except Exception:
            pass
    return _db


def get_accounts() -> list[dict]:
    db = _get_db()
    return [{"email": a["email"], "is_default": a.get("is_default", False), "sort_order": a.get("sort_order", 99)}
            for a in db["accounts"].find({}, {"email": 1, "is_default": 1, "sort_order": 1}).sort("sort_order", 1)]


def get_default_account() -> str | None:
    db = _get_db()
    doc = db["accounts"].find_one({"is_default": True})
    return doc["email"] if doc else None


def set_default_account(email: str):
    db = _get_db()
    db["accounts"].update_many({}, {"$set": {"is_default": False}})
    db["accounts"].update_one({"email": email}, {"$set": {"is_default": True}})


def add_account(email: str, password: str = "", is_default: bool = False, sort_order: int = 99):
    db = _get_db()
    if is_default:
        db["accounts"].update_many({}, {"$set": {"is_default": False}})
    db["accounts"].update_one(
        {"email": email},
        {"$set": {"email": email, "password": password, "is_default": is_default, "sort_order": sort_order}},
        upsert=True,
    )


def remove_account(email: str):
    db = _get_db()
    db["accounts"].delete_one({"email": email})


def save_gcloud_config(config_dir: str):
    if not os.path.exists(config_dir):
        return

    db = _get_db()

    for db_name in ["credentials.db", "access_tokens.db", "default_configs.db"]:
        fpath = os.path.join(config_dir, db_name)
        if os.path.exists(fpath):
            with open(fpath, "rb") as f:
                raw = f.read()
            db["files"].update_one(
                {"name": db_name},
                {"$set": {"name": db_name, "data": base64.b64encode(raw).decode()}},
                upsert=True,
            )

    text_files = {}
    for fname in ["active_config", "gce", "configurations/config_default"]:
        fpath = os.path.join(config_dir, fname)
        if os.path.exists(fpath):
            with open(fpath) as f:
                text_files[fname] = f.read()

    legacy = {}
    legacy_dir = os.path.join(config_dir, "legacy_credentials")
    if os.path.exists(legacy_dir):
        for email_dir in os.listdir(legacy_dir):
            ed = os.path.join(legacy_dir, email_dir)
            if os.path.isdir(ed):
                for fname in os.listdir(ed):
                    fpath = os.path.join(ed, fname)
                    if os.path.isfile(fpath):
                        with open(fpath) as f:
                            legacy[f"{email_dir}/{fname}"] = f.read()

    db["files"].update_one(
        {"name": "text_files"},
        {"$set": {"name": "text_files", "data": json.dumps(text_files)}},
        upsert=True,
    )
    db["files"].update_one(
        {"name": "legacy_files"},
        {"$set": {"name": "legacy_files", "data": json.dumps(legacy)}},
        upsert=True,
    )
    logger.info("Saved gcloud config to MongoDB")


def restore_gcloud_config(config_dir: str):
    os.makedirs(config_dir, exist_ok=True)
    db = _get_db()

    for db_name in ["credentials.db", "access_tokens.db", "default_configs.db"]:
        doc = db["files"].find_one({"name": db_name})
        if doc and "data" in doc:
            fpath = os.path.join(config_dir, db_name)
            raw = base64.b64decode(doc["data"])
            with open(fpath, "wb") as f:
                f.write(raw)
            logger.info("Restored %s (%d bytes)", db_name, len(raw))

    text_doc = db["files"].find_one({"name": "text_files"})
    if text_doc:
        text_files = json.loads(text_doc["data"])
        for fname, content in text_files.items():
            fpath = os.path.join(config_dir, fname)
            os.makedirs(os.path.dirname(fpath) or config_dir, exist_ok=True)
            with open(fpath, "w") as f:
                f.write(content)
        logger.info("Restored %d text files", len(text_files))

    legacy_doc = db["files"].find_one({"name": "legacy_files"})
    if legacy_doc:
        legacy = json.loads(legacy_doc["data"])
        for fname, content in legacy.items():
            fpath = os.path.join(config_dir, "legacy_credentials", fname)
            os.makedirs(os.path.dirname(fpath), exist_ok=True)
            with open(fpath, "w") as f:
                f.write(content)
        logger.info("Restored %d legacy files", len(legacy))

    os.chmod(config_dir, 0o700)
    logger.info("Restored gcloud config from MongoDB")


def save_start_script(content: str):
    db = _get_db()
    db["files"].update_one(
        {"name": "start_sh"},
        {"$set": {"name": "start_sh", "data": content}},
        upsert=True,
    )
    logger.info("Saved start.sh to MongoDB")


def get_start_script() -> str | None:
    db = _get_db()
    doc = db["files"].find_one({"name": "start_sh"})
    return doc["data"] if doc else None
