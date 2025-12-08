from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Generator
import os

# Database URL: uses environment variable or defaults to Docker service name 'db'
# For local development outside Docker, set DATABASE_URL="postgresql://user:password@localhost:5432/realestate_db"
SQLALCHEMY_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@db:5432/realestate_db"
)

# Create the SQLAlchemy engine. 
# 'pool_pre_ping=True' is crucial for long-running production applications 
# to ensure connections stay alive.
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, 
    pool_pre_ping=True
)

# Create a customized Session class. We will instantiate this when we need a session.
SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine
)

def get_db() -> Generator:
    """
    Dependency generator for database sessions.
    This function creates a new session for each request and closes it afterwards.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()