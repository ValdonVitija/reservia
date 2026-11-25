from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from reservation_ingest.config import Settings

Sessions = async_sessionmaker[AsyncSession]


def create_database(settings: Settings) -> tuple[AsyncEngine, Sessions]:
    engine = create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=0,
        pool_timeout=settings.db_pool_timeout,
        pool_pre_ping=True,
        hide_parameters=True,
        connect_args={
            "connect_timeout": 3,
            "options": "-c statement_timeout=10000 -c lock_timeout=4000",
        },
    )
    return engine, async_sessionmaker(engine, expire_on_commit=False)
