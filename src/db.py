"""
db.py — single source of truth for the database connection.

Same pattern as the sibling ecommerce-data-warehouse project: every other
module asks THIS file for an engine instead of building its own connection
string, so switching SQLite -> Postgres is a one-line environment variable
change, not a code change.

Default: SQLite file at db/supply_chain.db (zero setup).
Production: set DATABASE_URL, e.g.
    export DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/supply_chain"
See docker/docker-compose.yml for a ready-to-run local Postgres instance.
"""

import os
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE_PATH = PROJECT_ROOT / "db" / "supply_chain.db"
DEFAULT_DATABASE_URL = f"sqlite:///{DEFAULT_SQLITE_PATH}"

DATABASE_URL = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)


def get_engine(echo: bool = False):
    connect_args = {}
    if DATABASE_URL.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
    return create_engine(DATABASE_URL, echo=echo, connect_args=connect_args)


def get_session_factory(engine=None) -> sessionmaker:
    engine = engine or get_engine()
    return sessionmaker(bind=engine, expire_on_commit=False)


def get_session(engine=None) -> Session:
    return get_session_factory(engine)()
