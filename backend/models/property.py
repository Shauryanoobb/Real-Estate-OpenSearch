from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from enum import Enum

# --- Enumerations for Constrained Choices ---

class PropertyType(str, Enum):
    """Enumeration for classifying the type of real estate."""
    FLAT = "Flat"
    BUNGALOW = "Bungalow"
    PLOT = "Plot"
    OFFICE = "Office"
    SHOP = "Shop"
    AGRICULTURAL_LAND = "Agricultural Land"
    INDUSTRIAL_LAND = "Industrial Land"

class FurnishingStatus(str, Enum):
    """Enumeration for property furnishing level."""
    FURNISHED = "FURNISHED"
    UNFURNISHED = "UNFURNISHED"
    SEMI_FURNISHED = "SEMI_FURNISHED"

class Overlooking(str, Enum):
    """Enumeration for views from the property."""
    PARK = "Park"
    MAIN_ROAD = "Main Road"
    GARDEN = "Garden"
    POOL = "Pool"

class AdditionalRoom(str, Enum):
    """Enumeration for possible extra rooms."""
    STORE_ROOM = "Store Room"
    STUDY_ROOM = "Study Room"
    SERVANT_ROOM = "Servant Room"
    POOJA_ROOM = "Pooja Room"

# --- Main Property Model ---
# kept everything optional right now, confirm datatypes and decide enums
class Property(BaseModel):
    """The main property listing model, including embedded customer/seller details."""
    property_id: Optional[str] = None
    title: Optional[str] = None
    
    # Using Enums for validation and limited choices
    property_type: Optional[PropertyType] = None
    furnishing_status: Optional[FurnishingStatus] = None
    
    description: Optional[str] = None
    locality: Optional[str] = None
    price: Optional[float] = None
    bhk: Optional[int] = None
    facing_direction: Optional[str] = None
    area_sqft: Optional[int] = None
    bathrooms: Optional[int] = None
    
    # Optional[List[Enum]] allows multiple selections for views/rooms
    overlooking: Optional[List[Overlooking]] = []
    additional_rooms: Optional[List[AdditionalRoom]] = []

    amenities: Optional[List[str]] = []
    images: Optional[List[str]] = [] 

    listed_date: Optional[str] = None
    age_of_building: Optional[int] = None
    lift_available: Optional[bool] = None
    floor_number: Optional[int] = None
    total_floors: Optional[int] = None

    # --- Flattened Customer/Seller Details (One-to-One) ---
    # These fields are now direct properties of the Property table/document.
    seller_name: Optional[str] = None
    seller_email: Optional[str] = None
    seller_phone: Optional[str] = None
    seller_address: Optional[str] = None
    seller_referred_by: Optional[str] = None
    seller_additional_info: Optional[str] = None