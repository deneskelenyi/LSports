#!/usr/bin/python3
import os
from pathlib import Path

import mysql.connector
import pika
from requests.structures import CaseInsensitiveDict


BASE_DIR = Path(__file__).resolve().parent
_ENV_LOADED = False


def load_env(env_path: Path | None = None) -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return

    env_file = env_path or (BASE_DIR / ".env")
    if env_file.exists():
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)

    _ENV_LOADED = True


def env(key: str, default: str = "") -> str:
    load_env()
    return os.getenv(key, default)


def env_int(key: str, default: int) -> int:
    value = env(key, str(default))
    try:
        return int(value)
    except ValueError:
        return default


def mysql_connect_kwargs() -> dict:
    kwargs = {
        "user": env("MYSQL_USER"),
        "passwd": env("MYSQL_PASSWORD", ""),
        "database": env("MYSQL_DATABASE", "linesdb"),
    }
    unix_socket = env("MYSQL_UNIX_SOCKET")
    if unix_socket:
        kwargs["unix_socket"] = unix_socket
    else:
        kwargs["host"] = env("MYSQL_HOST", "localhost")
        kwargs["port"] = env_int("MYSQL_PORT", 3306)
    return kwargs


def create_mysql_connection():
    return mysql.connector.connect(**mysql_connect_kwargs())


def mqtt_host() -> str:
    return env("MQTT_HOST", "localhost")


def mqtt_port() -> int:
    return env_int("MQTT_PORT", 1883)


def memcache_address() -> tuple[str, int]:
    return env("MEMCACHE_HOST", "localhost"), env_int("MEMCACHE_PORT", 11211)


def lsports_headers() -> CaseInsensitiveDict:
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    return headers


def lsports_api_base_url() -> str:
    return env("LSPORTS_API_BASE_URL", "http://prematch.lsports.eu/OddService/")


def lsports_api_query() -> str:
    return (
        f"?username={env('LSPORTS_API_USERNAME')}"
        f"&password={env('LSPORTS_API_PASSWORD')}"
        f"&guid={env('LSPORTS_API_GUID')}"
    )


def lsports_package_id() -> str:
    return env("LSPORTS_PACKAGE_ID", "4620")


def lsports_queue_name() -> str:
    return env("LSPORTS_QUEUE_NAME", f"_{lsports_package_id()}_")


def lsports_remote_rabbitmq_params(connection_name: str | None = None, heartbeat: int = 20):
    params = {
        "host": env("LSPORTS_RMQ_HOST", "prematch-rmq.lsports.eu"),
        "port": env_int("LSPORTS_RMQ_PORT", 5672),
        "virtual_host": env("LSPORTS_RMQ_VHOST", "Customers"),
        "credentials": pika.PlainCredentials(
            username=env("LSPORTS_RMQ_USERNAME"),
            password=env("LSPORTS_RMQ_PASSWORD"),
            erase_on_connect=False,
        ),
        "heartbeat": heartbeat,
    }
    if connection_name:
        params["client_properties"] = {"connection_name": connection_name}
    return pika.ConnectionParameters(**params)


def local_rabbitmq_params(connection_name: str | None = None, heartbeat: int = 20):
    params = {
        "host": env("LOCAL_RMQ_HOST", "localhost"),
        "port": env_int("LOCAL_RMQ_PORT", 5672),
        "credentials": pika.PlainCredentials(
            username=env("LOCAL_RMQ_USERNAME"),
            password=env("LOCAL_RMQ_PASSWORD"),
            erase_on_connect=False,
        ),
        "heartbeat": heartbeat,
    }
    if connection_name:
        params["client_properties"] = {"connection_name": connection_name}
    return pika.ConnectionParameters(**params)


def publish_url(is_feed_forward: bool) -> str:
    key = "PUBLISH_URL_FF" if is_feed_forward else "PUBLISH_URL_DEFAULT"
    default = (
        "http://localhost/receiver_ff.php?base=1"
        if is_feed_forward
        else "http://localhost/receiver_v2_nowait.php?base=1"
    )
    return env(key, default)


def shovel_publish_hosts() -> tuple[str, str]:
    return (
        env("SHOVEL_PRIMARY_HOST", "localhost"),
        env("SHOVEL_SECONDARY_HOST", "localhost"),
    )
