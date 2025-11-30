from sqlalchemy import Column, Integer, String, Float, Boolean, ARRAY
from sqlalchemy.orm import declarative_base
from typing import Optional, List

# Base for SQLAlchemy models
Base = declarative_base()

class SQLProperty(Base):
    __tablename__ = "properties"

    # Primary key in the SQL DB. We can use this as the OpenSearch document ID.
    id = Column(String, primary_key=True) 
    
    # Required Fields
    title = Column(String, nullable=False)
    locality = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    bhk = Column(Integer, nullable=False)
    bathrooms = Column(Integer, nullable=False)
    area_sqft = Column(Integer, nullable=False)
    property_type = Column(String, nullable=False)

    # Optional Fields
    description = Column(String)
    city = Column(String)
    state = Column(String)
    furnishing_status = Column(String)
    # Using ARRAY for lists (specific to PostgreSQL)
    amenities = Column(ARRAY(String)) 
    listed_date = Column(String)
    available_from = Column(String)
    age_of_building = Column(Integer)
    lift_available = Column(Boolean)
    floor_number = Column(Integer)
    no_of_toilets = Column(Integer)
    balconies = Column(Integer)
    # geo_location (omitted for simplicity, would be a separate column type like Geography)

    def to_dict(self):
        # Convert the SQL object to a dict matching the Pydantic/OpenSearch structure
        data = self.__dict__.copy()
        data['property_id'] = data.pop('id') # Map SQL 'id' to OpenSearch 'property_id' field
        data.pop('_sa_instance_state', None) # Remove SQLAlchemy internal metadata
        return data