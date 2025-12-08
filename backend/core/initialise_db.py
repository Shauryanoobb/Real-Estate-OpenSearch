from sqlalchemy_utils import database_exists, create_database
from core.database_client import engine, SQLALCHEMY_DATABASE_URL
from models.sql_property import Base as PropertyBase  # Property models Base
from models.user import Base as UserBase  # User model Base

#python -m backend.core.initialise_db to run this file directly
#on changin an enum , you will have to manually drop that enum datatypoe like this DROP TYPE property_types;
#hence setting up good migration practice needed like alembic
def initialize_db():
    """Checks if the DB exists, creates it if necessary, and ensures all tables are created."""
    try:
        if not database_exists(SQLALCHEMY_DATABASE_URL):
            print("Database not found. Creating database...")
            create_database(SQLALCHEMY_DATABASE_URL)

        print("Creating/Ensuring all tables exist...")

        # Create property tables (supply_properties, demand_requests)
        PropertyBase.metadata.create_all(bind=engine)
        print("✓ Property tables created (supply_properties, demand_requests)")

        # Create user table (users)
        UserBase.metadata.create_all(bind=engine)
        print("✓ User table created (users)")

        print("Database initialization complete.")

    except Exception as e:
        print(f"Error during database initialization: {e}")

if __name__ == "__main__":
    # You can run this file directly to set up your DB
    initialize_db()