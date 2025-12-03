from sqlalchemy import Column, Integer, String, Float, Boolean, ARRAY, Enum
from sqlalchemy.orm import declarative_base
from typing import Optional, List
import uuid # Needed for default factory, assuming this import exists nearby

#this script doesnt overwrite , so first drop table
# Base for SQLAlchemy models
Base = declarative_base()

# --- 1. SQL Property Model (Source of Truth - Single Table) ---
class SQLProperty(Base):
    __tablename__ = "properties2"

    # Primary key, matching the OpenSearch ID
    id = Column(String, primary_key=True) 
    
    title = Column(String)
    locality = Column(String)
    price = Column(Float)
    bhk = Column(Integer)
    bathrooms = Column(Integer)
    area_sqft = Column(Integer)
    
    # Using SQLAlchemy's Enum type (ensure the values match the Python Enum strings)
    property_type = Column(Enum('Flat', 'Bungalow', 'Plot', 'Office', 'Shop', 'Agricultural Land', 'Industrial Land', name="property_types"))
    listing_type = Column(Enum('SALE', 'RENT', name="listing_types"))
    deposit = Column(Float)  # Only for RENT listings, will be NULL for SALE listings
    description = Column(String)
    facing_direction = Column(String)
    
    # Furnishing Status (Mapped from Python Enum)
    furnishing_status = Column(Enum('FURNISHED', 'UNFURNISHED', 'SEMI_FURNISHED', name="furnishing_statuses"))
    
    # ARRAY fields for multiple selections (PostgreSQL specific)
    amenities = Column(ARRAY(String)) 
    images = Column(ARRAY(String)) 
    overlooking = Column(ARRAY(String))      # Stores List[Overlooking Enum values]
    additional_rooms = Column(ARRAY(String)) # Stores List[AdditionalRoom Enum values]

    listed_date = Column(String)
    age_of_building = Column(Integer)
    lift_available = Column(Boolean)
    floor_number = Column(Integer)
    total_floors = Column(Integer)
    
    # --- Flattened Customer/Seller Details (Non-Relational) ---
    seller_name = Column(String)
    seller_email = Column(String)
    seller_phone = Column(String)
    seller_address = Column(String)
    seller_referred_by = Column(String)
    seller_additional_info = Column(String) # Added the new field

    def to_dict(self):
        # Convert the SQL object to a dict matching the Pydantic/OpenSearch structure
        data = self.__dict__.copy()
        data['property_id'] = data.pop('id') 
        data.pop('_sa_instance_state', None) # Remove SQLAlchemy internal metadata
        
        # When fetching data from DB, SQLAlchemy Enum values are automatically 
        # converted back to Python Enum strings if used correctly in Pydantic.

        return data