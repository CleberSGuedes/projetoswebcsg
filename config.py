import os
from pathlib import Path
from urllib.parse import quote_plus
from dotenv import load_dotenv

ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)


def _get_first(*names: str, default: str | None = None) -> str:
    for name in names:
        val = os.getenv(name)
        if val is not None and str(val).strip() != "":
            return val.strip()
    if default is not None:
        return default
    raise RuntimeError(f"Variavel obrigatoria nao definida no .env: {names[0]}")


def _as_bool_no(val: str) -> bool:
    v = (val or "").strip().lower()
    return v in {"no", "false", "0", "nao", "n"}


def _has_mssql_env() -> bool:
    for name in ("DB_USER_HMG", "DB_PASSWORD_HMG", "DB_HOST_HMG", "DB_PORT_HMG", "DB_NAME_HMG"):
        val = os.getenv(name)
        if val is not None and str(val).strip() != "":
            return True
    return False


def build_mysql_sqlalchemy_uri() -> str:
    user = _get_first("DB_USER_CSG", "DB_USER")
    password = _get_first("DB_PASSWORD_CSG", "DB_PASSWORD")
    host = _get_first("DB_HOST_CSG", "DB_HOST")
    port = _get_first("DB_PORT_CSG", "DB_PORT", default="3306")
    dbname = _get_first("DB_NAME_CSG", "DB_NAME", default="proj5954_spo-csg")

    query = (os.getenv("DB_QUERY_STRING") or "charset=utf8mb4").strip()
    query_suffix = f"?{query}" if query else ""

    return (
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}@"
        f"{host}:{port}/{quote_plus(dbname)}{query_suffix}"
    )


def build_mssql_sqlalchemy_uri() -> str:
    user = _get_first("DB_USER_HMG", "DB_USER")
    password = _get_first("DB_PASSWORD_HMG", "DB_PASSWORD")
    host = _get_first("DB_HOST_HMG", "DB_HOST")
    port = _get_first("DB_PORT_HMG", "DB_PORT")
    dbname = _get_first("DB_NAME_HMG", "DB_NAME", default="spo-hmg")

    driver_env = os.getenv("DB_DRIVER", "ODBC+Driver+18+for+SQL+Server").strip()
    driver_name = driver_env.replace("+", " ")

    encrypt_no = _as_bool_no(os.getenv("DB_ENCRYPT", "yes"))
    server = f"{host},{port}"

    if encrypt_no:
        encrypt_part = "Encrypt=no;TrustServerCertificate=yes"
    else:
        encrypt_part = "Encrypt=yes;TrustServerCertificate=no"

    odbc_str = (
        f"DRIVER={{{driver_name}}};"
        f"SERVER={server};"
        f"DATABASE={dbname};"
        f"UID={user};"
        f"PWD={password};"
        f"{encrypt_part};"
    )

    return "mssql+pyodbc:///?odbc_connect=" + quote_plus(odbc_str)


class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key")
    SESSION_COOKIE_SAMESITE = "Lax"
    SESSION_COOKIE_SECURE = os.getenv("SESSION_COOKIE_SECURE", "false").lower() == "true"

    engine = (os.getenv("DB_ENGINE") or "mysql").strip().lower()
    if engine == "mssql" and _has_mssql_env():
        SQLALCHEMY_DATABASE_URI = build_mssql_sqlalchemy_uri()
    else:
        SQLALCHEMY_DATABASE_URI = build_mysql_sqlalchemy_uri()
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_recycle": int(os.getenv("DB_POOL_RECYCLE", "280")),
        "pool_timeout": int(os.getenv("DB_POOL_TIMEOUT", "30")),
    }

    MAIL_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    MAIL_PORT = int(os.getenv("SMTP_PORT", "587"))
    MAIL_USE_TLS = True
    MAIL_USE_SSL = False
    MAIL_USERNAME = os.getenv("EMAIL_ADDRESS")
    MAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    MAIL_DEFAULT_SENDER = (os.getenv("MAIL_DEFAULT_SENDER") or os.getenv("EMAIL_ADDRESS") or "").strip().rstrip(",")
