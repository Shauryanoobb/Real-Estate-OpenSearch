from fastapi import APIRouter, HTTPException
from ..core.opensearch_client import client #fetch the client attribute from opensearch_client.py
from ..models.property import Property

router = APIRouter(prefix="/api/properties", tags=["Properties"])

INDEX = "properties" #name of the index in OpenSearch

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

#main query for searching properties
@router.get("/search/")
def search_properties(
    locality: str = None,
    keywords: str = None,
    title_keywords: str = None,
    bhk: int = None,
    min_sqft: int = None,
    max_sqft: int = None,
    min_price: int = None,
    max_price: int = None,
    is_furnished: bool = None,
    has_lift: bool = None,
    size: int = 10
):
    query = {"bool": {"must": [], "should": [], "filter": []}}

    if locality:
        query["bool"]["must"].append({
            "match": {"locality": {"query": locality, "fuzziness": "AUTO"}}
        })

    if keywords:
        query["bool"]["should"].append({
            "match": {"description": {"query": keywords, "boost": 2}}
        })

    if title_keywords:
        query["bool"]["should"].append({
            "match": {"title": {"query": title_keywords, "boost": 3}}
        })

    if bhk:
        query["bool"]["filter"].append({"term": {"bhk": bhk}})

    if min_sqft or max_sqft:
        sqft_range = {}
        if min_sqft:
            sqft_range["gte"] = min_sqft
        if max_sqft:
            sqft_range["lte"] = max_sqft
        query["bool"]["filter"].append({"range": {"sqft": sqft_range}})

    if min_price or max_price:
        price_range = {}
        if min_price:
            price_range["gte"] = min_price
        if max_price:
            price_range["lte"] = max_price
        query["bool"]["filter"].append({"range": {"price": price_range}})

    if is_furnished is not None:
        query["bool"]["filter"].append({"term": {"furnished_or_unfurnished": is_furnished}})

    if has_lift is not None:
        query["bool"]["filter"].append({"term": {"lift_available": has_lift}})

    body = {"query": query, "size": size}
    response = client.search(index=INDEX, body=body)
    return response

