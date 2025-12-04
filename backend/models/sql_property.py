from sqlalchemy import Column, Integer, String, Float, Boolean, ARRAY, Enum
from sqlalchemy.orm import declarative_base
from typing import Optional, List
import uuid 

Base = declarative_base()

# --- 1. SQL Supply Model (Inventory) ---
class SQLSupplyProperty(Base):
    __tablename__ = "supply_properties" # RENAME existing table

    id = Column(String, primary_key=True) 
    
    # Core Fields
    title = Column(String)
    description = Column(String)
    locality = Column(String)
    
    # Numeric/Price
    price = Column(Float)
    deposit = Column(Float)
    bhk = Column(Integer)
    bathrooms = Column(Integer)
    area_sqft = Column(Integer)
    age_of_building = Column(Integer)
    floor_number = Column(Integer)
    total_floors = Column(Integer)
    
    # Enum Fields
    property_type = Column(Enum('Flat', 'Bungalow', 'Plot', 'Office', 'Shop', 'Agricultural Land', 'Industrial Land', name="property_types"))
    listing_type = Column(Enum('SALE', 'RENT', name="listing_types"))
    furnishing_status = Column(Enum('FURNISHED', 'UNFURNISHED', 'SEMI_FURNISHED', name="furnishing_statuses"))
    
    # String/Boolean
    facing_direction = Column(String)
    listed_date = Column(String)
    lift_available = Column(Boolean)

    # Array Fields (PostgreSQL specific)
    amenities = Column(ARRAY(String)) 
    images = Column(ARRAY(String)) 
    overlooking = Column(ARRAY(String))
    additional_rooms = Column(ARRAY(String)) 
    
    # Flattened Customer Details
    customer_name = Column(String)
    customer_email = Column(String)
    customer_phone = Column(String)
    customer_address = Column(String)
    customer_referred_by = Column(String)
    customer_additional_info = Column(String)

    def to_dict(self):
        data = self.__dict__.copy()
        data['property_id'] = data.pop('id') 
        data.pop('_sa_instance_state', None)
        return data

# --- 2. SQL Demand Model (Requests) ---
class SQLDemandRequest(Base):
    __tablename__ = "demand_requests" # NEW TABLE

    id = Column(String, primary_key=True) 
    
    # Core Fields (Inherited from BaseListing)
    title = Column(String)
    description = Column(String)
    locality = Column(String)
    
    # Enum Fields
    property_type = Column(Enum('Flat', 'Bungalow', 'Plot', 'Office', 'Shop', 'Agricultural Land', 'Industrial Land', name="property_types"))
    listing_type = Column(Enum('SALE', 'RENT', name="listing_types"))
    furnishing_status = Column(Enum('FURNISHED', 'UNFURNISHED', 'SEMI_FURNISHED', name="furnishing_statuses"))

    # Range Fields (Min/Max)
    price_min = Column(Float)
    price_max = Column(Float)
    deposit_max = Column(Float)
    bhk_min = Column(Integer)
    bhk_max = Column(Integer)
    area_sqft_min = Column(Integer)
    area_sqft_max = Column(Integer)
    
    # Boolean/Date/Array
    listed_date = Column(String)
    lift_available = Column(Boolean)
    amenities = Column(ARRAY(String)) 
    overlooking = Column(ARRAY(String))
    additional_rooms = Column(ARRAY(String))
    images = Column(ARRAY(String))
    facing_direction = Column(String)

    # Requester Details (Flattened Customer)
    customer_name = Column(String)
    customer_email = Column(String)
    customer_phone = Column(String)
    customer_address = Column(String)
    customer_referred_by = Column(String)
    customer_additional_info = Column(String)

    def to_dict(self):
        data = self.__dict__.copy()
        data['request_id'] = data.pop('id') # Using request_id for clarity
        data.pop('_sa_instance_state', None)
        return data