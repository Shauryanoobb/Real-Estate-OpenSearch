from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from uuid import uuid4
import logging
from ..core.opensearch_client import client
from ..core.database_client import get_db
from ..models.property import SupplyProperty, DemandRequest 
from ..models.sql_property import SQLSupplyProperty, SQLDemandRequest 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) 

router = APIRouter(prefix="/api", tags=["Real Estate"]) # General prefix

INDEX_SUPPLY = "supply_properties"
INDEX_DEMAND = "demand_requests"

# --- HELPER: Maps SQL to OpenSearch Document and Indexes ---

def prepare_and_index(sql_model, index_name: str, id_key: str):
    """Commits the document to the specified OpenSearch index."""
    opensearch_doc = sql_model.to_dict()
    opensearch_doc.pop('property_id', None) # Clean Pydantic field residue
    opensearch_doc.pop('request_id', None)  # Clean Pydantic field residue

    # The ID to use for OpenSearch is the SQL PK
    doc_id = getattr(sql_model, 'id')
    
    response = client.index(index=index_name, id=doc_id, body=opensearch_doc)
    
    return response["_id"]

# --- HELPER: Creates a search query based on a specific Supply/Demand object ---
#i think we need to work A LOT ON THIS, it has to be made softer, include should, ask popsi
def create_cross_search_query(target_index: str, source_model):
    """
    Creates an OpenSearch query in the target index based on the specs of the source model.
    """
    query = {"bool": {"must": [], "filter": []}}

    # 1. Matching core fields
    if source_model.locality:
        # Use locality analyzer for good matches
        query["bool"]["must"].append({
            "match": {"locality": {"query": source_model.locality, "fuzziness": "AUTO"}}
        })
        
    if source_model.property_type:
        query["bool"]["filter"].append({"term": {"property_type": source_model.property_type.value}})
        
    if source_model.listing_type:
        query["bool"]["filter"].append({"term": {"listing_type": source_model.listing_type.value}})

    # 2. Financial/Specification Matching (CRITICAL LOGIC)
    # yaha hi sabse zyada scope hai change ka
    if target_index == INDEX_DEMAND:
        # Case A: Matching SUPPLY (exact price/bhk) against DEMAND (range min/max)
        if source_model.price:
            query["bool"]["filter"].append({"range": {"price_min": {"lte": source_model.price}}})
            query["bool"]["filter"].append({"range": {"price_max": {"gte": source_model.price}}})

        if source_model.bhk:
            query["bool"]["filter"].append({"range": {"bhk_min": {"lte": source_model.bhk}}})
            query["bool"]["filter"].append({"range": {"bhk_max": {"gte": source_model.bhk}}})
            
    elif target_index == INDEX_SUPPLY:
        # Case B: Matching DEMAND (range min/max) against SUPPLY (exact price/bhk)
        if source_model.price_min:
            query["bool"]["filter"].append({"range": {"price": {"gte": source_model.price_min}}})
        if source_model.price_max:
            query["bool"]["filter"].append({"range": {"price": {"lte": source_model.price_max}}})
        
        if source_model.bhk_min:
            query["bool"]["filter"].append({"range": {"bhk": {"gte": source_model.bhk_min}}})
        if source_model.bhk_max:
            query["bool"]["filter"].append({"range": {"bhk": {"lte": source_model.bhk_max}}})
            
    # Add other filters if needed (e.g., amenities, lift)
    
    return {"query": query, "size": 10} # Limit matches to 10


# --- 1. SUPPLY ENDPOINTS (/api/supply) ---

@router.post("/supply/")
def add_supply(property_data: SupplyProperty, db: Session = Depends(get_db)):
    
    new_id = property_data.property_id if property_data.property_id else str(uuid4())
    sql_data = property_data.dict(exclude_none=True)
    sql_data["id"] = new_id
    sql_data.pop("property_id", None)
    
    sql_model = SQLSupplyProperty(**sql_data)
    
    try:
        # 1. DB COMMIT
        db.add(sql_model)
        db.commit()
        db.refresh(sql_model)
        
        # 2. OPENSEARCH INDEXING
        opensearch_id = prepare_and_index(sql_model, INDEX_SUPPLY, 'id')
        
        # 3. CROSS-SEARCH (Back Search: Match new Supply against existing Demand)
        search_body = create_cross_search_query(INDEX_DEMAND, property_data)
        matching_demand = client.search(index=INDEX_DEMAND, body=search_body)
        
    except Exception as e:
        db.rollback() 
        logger.error(f"Error saving/indexing supply property: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving/indexing property: {e}")
    
    return {
        "result": "Supply added", 
        "opensearch_id": opensearch_id, 
        "matches": matching_demand.get('hits', {}).get('hits', [])
    }

# NEW: Update Supply Property
@router.put("/supply/{property_id}")
def update_supply(property_id: str, property_data: SupplyProperty, db: Session = Depends(get_db)):
    
    # 1. CHECK & FETCH POSTGRESQL
    sql_model = db.query(SQLSupplyProperty).filter(SQLSupplyProperty.id == property_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Supply Property not found in DB")
        
    update_data = property_data.dict(exclude_none=True)
    
    try:
        # 2. UPDATE POSTGRESQL
        for key, value in update_data.items():
            if hasattr(sql_model, key) and key not in ('id', 'property_id'):
                setattr(sql_model, key, value)
        
        db.commit()
        db.refresh(sql_model)
        
        # 3. UPDATE OPENSEARCH (Synchronization step)
        # FIX: Capture the returned ID from the helper function
        opensearch_id = prepare_and_index(sql_model, INDEX_SUPPLY, 'id')
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error saving/indexing supply property: {e}")
        raise HTTPException(status_code=500, detail=f"Database or Indexing error during update: {e}")
    
    # FIX: The opensearch_id variable is now correctly defined here
    return {"result": "Supply updated", "opensearch_id": opensearch_id}


# GET /api/search/supply/ 
#here we perform a very simple query: hard filters on locality, property type, furnishing status, bhk, lift availability
#and range filters on sqft and price (hard), only soft match on keywords (title, description)
#add listing type
@router.get("/search/supply/")
def search_supply_properties(
    locality: str = None,
    keywords: str = None,
    title_keywords: str = None,
    listing_type: str = None,
    property_type: str = None, 
    facing_direction: str = None,
    furnishing_status: str = None,
    bhk: int = None,
    min_sqft: int = None,
    max_sqft: int = None,
    min_price: int = None,
    max_price: int = None,
    has_lift: bool = None,
    customer_name: str = None,
    size: int = 10
):
    query = {"bool": {"must": [], "should": [], "filter": []}}
    # --- Standard OpenSearch Query Building (Use INDEX_SUPPLY) ---
    
    if locality: query["bool"]["must"].append({"match": {"locality": {"query": locality, "fuzziness": "AUTO"}}})
    if keywords: query["bool"]["should"].append({"match": {"description": {"query": keywords, "boost": 2}}})
    if title_keywords: query["bool"]["should"].append({"match": {"title": {"query": title_keywords, "boost": 3}}})
    
    # Term Filters
    if property_type: query["bool"]["filter"].append({"term": {"property_type": property_type}})
    if furnishing_status: query["bool"]["filter"].append({"term": {"furnishing_status": furnishing_status}})
    if bhk: query["bool"]["filter"].append({"term": {"bhk": bhk}})
    if has_lift is not None: query["bool"]["filter"].append({"term": {"lift_available": has_lift}})
    if listing_type: query["bool"]["filter"].append({"term": {"listing_type": listing_type}})
    if facing_direction: query["bool"]["filter"].append({"term": {"facing_direction": facing_direction}})
    if customer_name: query["bool"]["filter"].append({"term": {"customer_name": customer_name}}) #move away from keyword

    # Range Filters
    if min_sqft or max_sqft:
        sqft_range = {}
        if min_sqft: sqft_range["gte"] = min_sqft
        if max_sqft: sqft_range["lte"] = max_sqft
        query["bool"]["filter"].append({"range": {"area_sqft": sqft_range}})

    if min_price or max_price:
        price_range = {}
        if min_price: price_range["gte"] = min_price
        if max_price: price_range["lte"] = max_price
        query["bool"]["filter"].append({"range": {"price": price_range}})

    body = {"query": query, "size": size}
    response = client.search(index=INDEX_SUPPLY, body=body)
    return response


# --- 2. DEMAND ENDPOINTS (/api/demand) ---

@router.post("/demand/")
def add_demand(request_data: DemandRequest, db: Session = Depends(get_db)):
    
    new_id = request_data.property_id if request_data.property_id else str(uuid4()) # Using property_id slot for request ID
    sql_data = request_data.dict(exclude_none=True)
    sql_data["id"] = new_id
    sql_data.pop("property_id", None) 
    
    sql_model = SQLDemandRequest(**sql_data)
    
    try:
        # 1. DB COMMIT
        db.add(sql_model)
        db.commit()
        db.refresh(sql_model)
        
        # 2. OPENSEARCH INDEXING
        opensearch_id = prepare_and_index(sql_model, INDEX_DEMAND, 'id')
        
        # 3. CROSS-SEARCH (Back Search: Match new Demand against existing Supply)
        search_body = create_cross_search_query(INDEX_SUPPLY, request_data)
        matching_supply = client.search(index=INDEX_SUPPLY, body=search_body)
        
    except Exception as e:
        db.rollback() 
        logger.error(f"Error saving/indexing demand request: {e}")
        raise HTTPException(status_code=500, detail=f"Error saving/indexing demand request: {e}")
    
    return {
        "result": "Demand added", 
        "opensearch_id": opensearch_id, 
        "matches": matching_supply.get('hits', {}).get('hits', [])
    }

# NEW: Update Demand Property
@router.put("/demand/{request_id}")
def update_demand(request_id: str, request_data: DemandRequest, db: Session = Depends(get_db)):
    
    # 1. CHECK & FETCH POSTGRESQL
    sql_model = db.query(SQLDemandRequest).filter(SQLDemandRequest.id == request_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Demand Request not found in DB")
        
    update_data = request_data.dict(exclude_none=True)
    
    try:
        # 2. UPDATE POSTGRESQL
        for key, value in update_data.items():
            if hasattr(sql_model, key) and key not in ('id', 'property_id'):
                setattr(sql_model, key, value)
        
        db.commit()
        db.refresh(sql_model)
        
        # 3. UPDATE OPENSEARCH (Synchronization step)
        # FIX: Capture the returned ID from the helper function
        opensearch_id = prepare_and_index(sql_model, INDEX_DEMAND, 'id')
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error saving/indexing demand request: {e}")
        raise HTTPException(status_code=500, detail=f"Database or Indexing error during update: {e}")
    
    # FIX: The opensearch_id variable is now correctly defined here
    return {"result": "Demand updated", "opensearch_id": opensearch_id}

# --- READ ENDPOINTS (READ from DB for freshness or OpenSearch for search) ---

# Get Supply by ID
@router.get("/supply/{property_id}")
def get_supply_property(property_id: str, db: Session = Depends(get_db)):
    sql_model = db.query(SQLSupplyProperty).filter(SQLSupplyProperty.id == property_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Supply Property not found")
    return SupplyProperty(**sql_model.to_dict()) # Return as Pydantic type

# Get Demand by ID
@router.get("/demand/{request_id}")
def get_demand_request(request_id: str, db: Session = Depends(get_db)):
    sql_model = db.query(SQLDemandRequest).filter(SQLDemandRequest.id == request_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Demand Request not found")
    return DemandRequest(**sql_model.to_dict()) # Return as Pydantic type

# Get All Supply
@router.get("/supply/")
def get_all_supply(size: int = 10):
    response = client.search(index=INDEX_SUPPLY, body={"query": {"match_all": {}}, "size": size})
    return response

# Get All Demand
@router.get("/demand/")
def get_all_demand(size: int = 10):
    response = client.search(index=INDEX_DEMAND, body={"query": {"match_all": {}}, "size": size})
    return response

# Delete Supply
@router.delete("/supply/{property_id}")
def delete_supply(property_id: str, db: Session = Depends(get_db)):
    sql_model = db.query(SQLSupplyProperty).filter(SQLSupplyProperty.id == property_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Supply Property not found")
    try:
        db.delete(sql_model)
        db.commit()
        response = client.delete(index=INDEX_SUPPLY, id=property_id, ignore=[404])
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database or Indexing error during deletion: {e}")
    return {"result": "Supply deleted", "opensearch_response": response.get('result')}

# Delete Demand
@router.delete("/demand/{request_id}")
def delete_demand(request_id: str, db: Session = Depends(get_db)):
    sql_model = db.query(SQLDemandRequest).filter(SQLDemandRequest.id == request_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Demand Request not found")
    try:
        db.delete(sql_model)
        db.commit()
        response = client.delete(index=INDEX_DEMAND, id=request_id, ignore=[404])
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database or Indexing error during deletion: {e}")
    return {"result": "Demand deleted", "opensearch_response": response.get('result')}

#TERM AND RANGE PERFORM HARD SEARCHES
# GET /api/search/supply/ 
@router.get("/search/supply/")
def search_supply_properties(
    locality: str = None,
    keywords: str = None,
    title_keywords: str = None,
    property_type: str = None, 
    furnishing_status: str = None,
    bhk: int = None,
    min_sqft: int = None,
    max_sqft: int = None,
    min_price: int = None,
    max_price: int = None,
    has_lift: bool = None,
    size: int = 10
):
    query = {"bool": {"must": [], "should": [], "filter": []}}
    # --- Standard OpenSearch Query Building (Use INDEX_SUPPLY) ---
    
    if locality: query["bool"]["must"].append({"match": {"locality": {"query": locality, "fuzziness": "AUTO"}}})
    if keywords: query["bool"]["should"].append({"match": {"description": {"query": keywords, "boost": 2}}})
    if title_keywords: query["bool"]["should"].append({"match": {"title": {"query": title_keywords, "boost": 3}}})
    
    # Term Filters
    if property_type: query["bool"]["filter"].append({"term": {"property_type": property_type}})
    if furnishing_status: query["bool"]["filter"].append({"term": {"furnishing_status": furnishing_status}})
    if bhk: query["bool"]["filter"].append({"term": {"bhk": bhk}})
    if has_lift is not None: query["bool"]["filter"].append({"term": {"lift_available": has_lift}})

    # Range Filters
    if min_sqft or max_sqft:
        sqft_range = {}
        if min_sqft: sqft_range["gte"] = min_sqft
        if max_sqft: sqft_range["lte"] = max_sqft
        query["bool"]["filter"].append({"range": {"area_sqft": sqft_range}})

    if min_price or max_price:
        price_range = {}
        if min_price: price_range["gte"] = min_price
        if max_price: price_range["lte"] = max_price
        query["bool"]["filter"].append({"range": {"price": price_range}})

    body = {"query": query, "size": size}
    response = client.search(index=INDEX_SUPPLY, body=body)
    return response

# GET /api/search/demand/
#see how to change, and what to remove because most of this will never be searched in demand 
@router.get("/search/demand/")
def search_demand_requests(
    locality: str = None,
    keywords: str = None,
    title_keywords: str = None,
    property_type: str = None, 
    furnishing_status: str = None,
    customer_name: str = None,
    listing_type: str = None,
    facing_direction: str = None,
    # Demand-specific search parameters
    bhk_min: int = None,
    bhk_max: int = None,
    min_sqft: int = None,
    max_sqft: int = None,
    min_price: int = None,
    max_price: int = None,
    has_lift: bool = None,
    size: int = 10
):
    query = {"bool": {"must": [], "should": [], "filter": []}}
    
    # --- Full Text Search ---
    if locality: query["bool"]["must"].append({"match": {"locality": {"query": locality, "fuzziness": "AUTO"}}})
    if keywords: query["bool"]["should"].append({"match": {"description": {"query": keywords, "boost": 2}}})
    if title_keywords: query["bool"]["should"].append({"match": {"title": {"query": title_keywords, "boost": 3}}})
    
    # Term Filters
    if property_type: query["bool"]["filter"].append({"term": {"property_type": property_type}})
    if furnishing_status: query["bool"]["filter"].append({"term": {"furnishing_status": furnishing_status}})
    if has_lift is not None: query["bool"]["filter"].append({"term": {"lift_available": has_lift}})
    if listing_type: query["bool"]["filter"].append({"term": {"listing_type": listing_type}})
    if facing_direction: query["bool"]["filter"].append({"term": {"facing_direction": facing_direction}})
    if customer_name: query["bool"]["filter"].append({"term": {"customer_name": customer_name}})

    # --- Range Filters (Demand uses Min/Max fields) ---
    
    # BHK Range
    if bhk_min or bhk_max:
        bhk_range = {}
        if bhk_min: bhk_range["gte"] = bhk_min
        if bhk_max: bhk_range["lte"] = bhk_max
        query["bool"]["filter"].append({"range": {"bhk_min": bhk_range}}) # NOTE: Filter uses the min field in the demand index

    # Area Range
    if min_sqft or max_sqft:
        sqft_range = {}
        if min_sqft: sqft_range["gte"] = min_sqft
        if max_sqft: sqft_range["lte"] = max_sqft
        #for a soft match on bhk range, use should
        # query["bool"]["should"].append({
        #     "range": {
        #         "bhk_min": bhk_range   # Demand index stores min BHK required
        #     }
        # })
        query["bool"]["filter"].append({"range": {"area_sqft_min": sqft_range}}) # NOTE: Filter uses the min field in the demand index

    # Price/Budget Range
    if min_price or max_price:
        price_range = {}
        if min_price: price_range["gte"] = min_price
        if max_price: price_range["lte"] = max_price
        query["bool"]["filter"].append({"range": {"price_min": price_range}}) # NOTE: Filter uses the min field in the demand index


    body = {"query": query, "size": size}
    response = client.search(index=INDEX_DEMAND, body=body)
    return response