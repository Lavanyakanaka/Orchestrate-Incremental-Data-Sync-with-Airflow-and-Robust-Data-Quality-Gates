"""
Database utilities for the analytical data warehouse.
Provides connection management, engine creation, and SQL execution utilities.
"""

import os
import logging
from typing import Optional
import psycopg2
from psycopg2.extensions import connection as PgConnection
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def get_db_credentials() -> dict:
    """
    Retrieve analytical database credentials from environment variables or Airflow connection.
    Falls back to environment variables first, then default Docker network values.
    """
    # Attempt to resolve from Airflow connection if running within an Airflow context
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection("analytical_dw_conn")
        return {
            "host": conn.host or os.getenv("ANALYTICAL_DB_HOST", "postgres-dw"),
            "port": conn.port or int(os.getenv("ANALYTICAL_DB_PORT", "5432")),
            "dbname": conn.schema or os.getenv("ANALYTICAL_DB_NAME", "analytical_dw"),
            "user": conn.login or os.getenv("ANALYTICAL_DB_USER", "dw_user"),
            "password": conn.password or os.getenv("ANALYTICAL_DB_PASSWORD", "dw_password"),
        }
    except Exception:
        # Fallback to direct environment variables
        return {
            "host": os.getenv("ANALYTICAL_DB_HOST", "postgres-dw"),
            "port": int(os.getenv("ANALYTICAL_DB_PORT", "5432")),
            "dbname": os.getenv("ANALYTICAL_DB_NAME", "analytical_dw"),
            "user": os.getenv("ANALYTICAL_DB_USER", "dw_user"),
            "password": os.getenv("ANALYTICAL_DB_PASSWORD", "dw_password"),
        }


def get_connection(override_creds: Optional[dict] = None) -> PgConnection:
    """
    Establish and return a psycopg2 database connection.
    """
    creds = override_creds or get_db_credentials()
    logger.info("Connecting to PostgreSQL DW at %s:%s/%s as %s",
                creds["host"], creds["port"], creds["dbname"], creds["user"])
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["user"],
        password=creds["password"],
    )
    conn.autocommit = False
    return conn


def get_engine(override_creds: Optional[dict] = None) -> Engine:
    """
    Create and return a SQLAlchemy engine for analytical database operations.
    """
    creds = override_creds or get_db_credentials()
    conn_url = (
        f"postgresql+psycopg2://{creds['user']}:{creds['password']}@"
        f"{creds['host']}:{creds['port']}/{creds['dbname']}"
    )
    return create_engine(conn_url, pool_pre_ping=True)


def execute_sql_file(file_path: str, conn: Optional[PgConnection] = None) -> None:
    """
    Execute all SQL statements in a given .sql file.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found at: {file_path}")

    logger.info("Executing SQL file: %s", file_path)
    with open(file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    close_conn = False
    if conn is None:
        conn = get_connection()
        close_conn = True

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_content)
        conn.commit()
        logger.info("Successfully executed %s", file_path)
    except Exception as e:
        conn.rollback()
        logger.error("Failed executing SQL file %s: %s", file_path, e)
        raise
    finally:
        if close_conn:
            conn.close()
