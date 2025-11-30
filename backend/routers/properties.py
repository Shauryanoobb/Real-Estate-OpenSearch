from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from uuid import uuid4
import logging
from ..core.opensearch_client import client # OpenSearch client
from ..core.database_client import get_db # Dependency for SQLAlchemy session
from ..models.property import Property as PydanticProperty # Pydantic model for input validation
from ..models.sql_property import SQLProperty # SQLAlchemy model for database mapping

router = APIRouter(prefix="/api/properties", tags=["Properties"])
# Configure logging at the module level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
INDEX = "properties"

# --- HELPER: Maps Pydantic/SQL to OpenSearch Document Structure ---

def prepare_opensearch_doc(sql_model: SQLProperty):
    """Converts the SQL model object to a dict ready for OpenSearch indexing."""
    opensearch_doc = sql_model.to_dict() # Assumes SQLProperty has a .to_dict() method
    
    # Remove the Pydantic property_id field, as the SQL model's 'id' is used as the document ID
    opensearch_doc.pop('property_id', None)
    
    return opensearch_doc

# --- CRUD ENDPOINTS (Dual Write Implemented) ---

# Add new property (CREATE)
@router.post("/")
def add_property(property_data: PydanticProperty, db: Session = Depends(get_db)):
    
    # 1. PREPARE DATA and GENERATE ID
    # Use user-provided ID if available, otherwise generate a UUID for the SQL Primary Key.
    new_id = property_data.property_id if property_data.property_id else str(uuid4())
    
    sql_data = property_data.dict(exclude_none=True)
    sql_data["id"] = new_id # Set the SQL Primary Key
    sql_data.pop("property_id", None) # Remove it, as 'id' is the canonical field
    
    sql_model = SQLProperty(**sql_data)
    logger.info(f"Attempting to save SQL Model with ID: {new_id}")
    logger.debug(f"SQL Data Payload: {sql_data}")
    try:
        # 2. SAVE TO POSTGRESQL (Source of Truth)
        db.add(sql_model)
        db.commit()
        db.refresh(sql_model)
        logger.info(f"Property added to DB with ID: {new_id}")
        # 3. INDEX IN OPENSEARCH (Synchronization step)
        opensearch_doc = prepare_opensearch_doc(sql_model)
        print(f"Indexing document in OpenSearch with ID: {new_id}")
        
        # Use SQL ID as the OpenSearch document ID
        response = client.index(index=INDEX, id=new_id, body=opensearch_doc)
        
    except Exception as e:
        db.rollback() # CRITICAL: Rollback SQL transaction on failure
        # Log the error here if running in production
        raise HTTPException(status_code=500, detail=f"Database or Indexing error during creation: {e}")
    
    return {"result": "Property added", "opensearch_id": response["_id"], "db_id": new_id}

# Update property (UPDATE)
@router.put("/{opensearch_id}")
def update_property(opensearch_id: str, property_data: PydanticProperty, db: Session = Depends(get_db)):
    
    # 1. CHECK & FETCH POSTGRESQL
    sql_model = db.query(SQLProperty).filter(SQLProperty.id == opensearch_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Property not found in DB")
        
    update_data = property_data.dict(exclude_none=True)
    
    try:
        # 2. UPDATE POSTGRESQL
        for key, value in update_data.items():
            if hasattr(sql_model, key) and key not in ('id', 'property_id'):
                setattr(sql_model, key, value)
        
        db.commit()
        db.refresh(sql_model)
        
        # 3. UPDATE OPENSEARCH (Synchronization step)
        opensearch_doc = prepare_opensearch_doc(sql_model)
        
        # Use client.index (full replacement) or client.update (partial)
        # client.index is simpler and safer for dual writes if the doc isn't huge
        response = client.index(index=INDEX, id=opensearch_id, body=opensearch_doc) 
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database or Indexing error during update: {e}")
    
    return {"result": "Property updated", "opensearch_id": response["_id"]}

# Delete property (DELETE)
@router.delete("/{property_id}")
def delete_property(property_id: str, db: Session = Depends(get_db)):
    
    # 1. DELETE FROM POSTGRESQL
    sql_model = db.query(SQLProperty).filter(SQLProperty.id == property_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Property not found in DB")
        
    try:
        db.delete(sql_model)
        db.commit()
        
        # 2. DELETE FROM OPENSEARCH
        response = client.delete(index=INDEX, id=property_id, ignore=[404])
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Database or Indexing error during deletion: {e}")
        
    return {"result": "Property deleted", "opensearch_response": response.get('result')}

# Get single property by ID (READ - Prioritize Database for Freshness)
@router.get("/{property_id}")
def get_property(property_id: str, db: Session = Depends(get_db)):
    # Read from DB for absolute latest details (better than relying on OpenSearch sync lag)
    sql_model = db.query(SQLProperty).filter(SQLProperty.id == property_id).first()
    if not sql_model:
        raise HTTPException(status_code=404, detail="Property not found")
        
    # Convert SQL model back to Pydantic-friendly dictionary structure
    return PydanticProperty(**sql_model.to_dict())

# Get all properties (READ - OpenSearch)
@router.get("/")
def get_all_properties(size: int = 10):
    # Read from OpenSearch for speed and simple global query
    response = client.search(index=INDEX, body={"query": {"match_all": {}}, "size": size})
    return response

# Search properties (READ - OpenSearch)
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
    # This remains unchanged, as OpenSearch is the system of choice for search.
    query = {"bool": {"must": [], "should": [], "filter": []}}

    if locality:
        query["bool"]["must"].append({
            "match": {"locality": {"query": locality, "fuzziness": "AUTO"}}
        })

    if keywords:
        query["bool"]["should"].append({
            "match": {"description": {"query": keywords, "boost": 2}}
        })
    # ... (rest of search query building logic) ...
    
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
        query["bool"]["filter"].append({"range": {"area_sqft": sqft_range}}) # NOTE: Changed sqft to area_sqft

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