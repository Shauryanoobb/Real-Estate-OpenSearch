from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Generator

# NOTE: Replace these with your actual PostgreSQL connection details and environment variables.
SQLALCHEMY_DATABASE_URL = "postgresql://user:password@localhost:5432/realestate_db" #change localhost to db, if running inside

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