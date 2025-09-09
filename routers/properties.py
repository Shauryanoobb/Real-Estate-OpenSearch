from fastapi import APIRouter, HTTPException
from core.opensearch_client import client
from models.property import Property

router = APIRouter(prefix="/properties", tags=["Properties"])

INDEX = "properties"

# Add new property
@router.post("/")
def add_property(property: Property):
    response = client.index(index=INDEX, id=property.property_id, body=property.dict())
    return {"result": "Property added", "response": response}

# Get all properties
@router.get("/")
def get_all_properties(size: int = 10):
    response = client.search(index=INDEX, body={"query": {"match_all": {}}, "size": size})
    return response

# Get single property by ID
@router.get("/{property_id}")
def get_property(property_id: str):
    response = client.get(index=INDEX, id=property_id, ignore=[404])
    if not response or response.get("found") is False:
        raise HTTPException(status_code=404, detail="Property not found")
    return response

# Update property
@router.put("/{property_id}")
def update_property(property_id: str, property: Property):
    response = client.update(index=INDEX, id=property_id, body={"doc": property.dict()})
    return {"result": "Property updated", "response": response}

# Delete property
@router.delete("/{property_id}")
def delete_property(property_id: str):
    response = client.delete(index=INDEX, id=property_id, ignore=[404])
    if response.get("result") == "not_found":
        raise HTTPException(status_code=404, detail="Property not found")
    return {"result": "Property deleted"}

# Search property by locality
@router.get("/search/")
def search_properties(locality: str, size: int = 5):
    query = {
        "query": {
            "match": {
                "locality": {
                    "query": locality,
                    "fuzziness": "AUTO"
                }
            }
        },
        "size": size
    }
    response = client.search(index=INDEX, body=query)
    return response
