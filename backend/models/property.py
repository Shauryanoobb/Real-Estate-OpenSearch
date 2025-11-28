from pydantic import BaseModel
from typing import List, Optional

class Property(BaseModel):
    property_id: Optional[str] = None
    title: str
    description: Optional[str] = None
    locality: str
    city: Optional[str] = None
    state: Optional[str] = None
    price: float
    bhk: int
    bathrooms: int
    area_sqft: int
    furnishing_status: Optional[str] = None
    furnished_or_unfurnished: Optional[bool] = None
    property_type: str
    amenities: Optional[List[str]] = []
    listed_date: Optional[str] = None
    available_from: Optional[str] = None
    age_of_building: Optional[int] = None
    lift_available: Optional[bool] = None
    floor_number: Optional[int] = None
    no_of_toilets: Optional[int] = None
    balconies: Optional[int] = None
    geo_location: Optional[dict] = None
