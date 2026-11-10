"""Engine creation and Alembic migration runner."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def make_engine(url: str, *, pool_size: int = 5, echo: bool = False) -> Engine:
    return create_engine(
        url,
        pool_size=pool_size,
        max_overflow=pool_size,
        pool_pre_ping=True,
        echo=echo,
    )


def _alembic_root() -> Path:
    # Repo checkout (tests, host) or /app in the container image.
    for candidate in (PROJECT_ROOT, Path("/app")):
        if (candidate / "alembic.ini").exists():
            return candidate
    raise FileNotFoundError("alembic.ini not found")


def run_migrations(url: str, revision: str = "head") -> None:
    root = _alembic_root()
    cfg = Config(str(root / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "alembic"))
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    command.upgrade(cfg, revision)
