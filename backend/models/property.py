from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from enum import Enum

# --- Enumerations for Constrained Choices ---

class PropertyType(str, Enum):
    FLAT = "Flat"
    BUNGALOW = "Bungalow"
    PLOT = "Plot"
    OFFICE = "Office"
    SHOP = "Shop"
    AGRICULTURAL_LAND = "Agricultural Land"
    INDUSTRIAL_LAND = "Industrial Land"

class ListingType(str, Enum):
    SALE = "SALE"
    RENT = "RENT"

class FurnishingStatus(str, Enum):
    FURNISHED = "FURNISHED"
    UNFURNISHED = "UNFURNISHED"
    SEMI_FURNISHED = "SEMI_FURNISHED"

class Overlooking(str, Enum):
    PARK = "Park"
    MAIN_ROAD = "Main Road"
    GARDEN = "Garden"
    POOL = "Pool"

class AdditionalRoom(str, Enum):
    STORE_ROOM = "Store Room"
    STUDY_ROOM = "Study Room"
    SERVANT_ROOM = "Servant Room"
    POOJA_ROOM = "Pooja Room"

# --- Base Fields (Shared between Supply and Demand) ---

class BaseListing(BaseModel):
    """Base model containing fields common to both inventory and demand requests."""

    # Optional fields for both, but must be present in the DB schema
    property_id: Optional[str] = None
    customer_id: Optional[str] = None  # User/Owner ID (set automatically by backend)
    title: Optional[str] = None
    description: Optional[str] = None
    locality: Optional[str] = None
    facing_direction: Optional[str] = None
    
    # Enum fields
    property_type: Optional[PropertyType] = None
    listing_type: Optional[ListingType] = None
    furnishing_status: Optional[FurnishingStatus] = None

    # Arrays
    overlooking: Optional[List[Overlooking]] = []
    additional_rooms: Optional[List[AdditionalRoom]] = []
    amenities: Optional[List[str]] = []

    listed_date: Optional[str] = None
    lift_available: Optional[bool] = None
    
    # Seller/Requester Details (Flattened)
    customer_name: Optional[str] = None # Renamed from seller_name
    customer_email: Optional[str] = None
    customer_phone: Optional[str] = None
    customer_address: Optional[str] = None
    customer_referred_by: Optional[str] = None
    customer_additional_info: Optional[str] = None

# --- 1. Supply (Inventory) Model ---

class SupplyProperty(BaseListing):
    """Represents a specific, available property for Sale or Rent."""
    
    price: Optional[float] = None
    deposit: Optional[float] = None  # Only for RENT listings

    bhk: Optional[int] = None
    area_sqft: Optional[int] = None
    # these fields are specific to supply properties
    bathrooms: Optional[int] = None
    age_of_building: Optional[int] = None
    floor_number: Optional[int] = None
    total_floors: Optional[int] = None

# --- 2. Demand (Request) Model ---

class DemandRequest(BaseListing):
    """Represents a client's requirements, using min/max ranges for search."""

    # Financial Ranges (Price/Budget)
    price_min: Optional[float] = None
    price_max: Optional[float] = None
    deposit_max: Optional[float] = None

    # Specification Ranges
    bhk_min: Optional[int] = None
    bhk_max: Optional[int] = None
    area_sqft_min: Optional[int] = None
    area_sqft_max: Optional[int] = None

    # Additional specs
    bathrooms: Optional[int] = None
