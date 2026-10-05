import argparse
from pathlib import Path

from alembic import command
from alembic.config import Config

from shared.settings import settings


def migration_config() -> Config:
    config = Config()
    migrations = Path(__file__).resolve().parent / "migrations"
    config.set_main_option("script_location", str(migrations).replace("%", "%%"))
    config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage payment database migrations")
    parser.add_argument("action", choices=("upgrade", "downgrade", "current", "history"))
    parser.add_argument("revision", nargs="?")
    arguments = parser.parse_args()
    config = migration_config()
    if arguments.action == "upgrade":
        command.upgrade(config, arguments.revision or "head")
    elif arguments.action == "downgrade":
        command.downgrade(config, arguments.revision or "-1")
    elif arguments.action == "current":
        command.current(config, verbose=True)
    else:
        command.history(config, verbose=True)


if __name__ == "__main__":
    main()
