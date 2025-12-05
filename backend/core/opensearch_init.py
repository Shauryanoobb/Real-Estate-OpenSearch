import json
from ..core.opensearch_client import client # Your existing OpenSearch client
import time

INDEX_SUPPLY = "supply_properties"
INDEX_DEMAND = "demand_requests"
#set up customer name with same settings for fuzzy search
# --- 1. Base Settings (Common to both indices) ---
ANALYSIS_SETTINGS = {
    "analysis": {
      "analyzer": {
        "fuzzy_search_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "asciifolding",
            "stemmer",
            "synonym_filter",
            "stop"
          ]
        },
        "autocomplete_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "asciifolding",
            "edge_ngram_filter"
          ]
        },
        "autocomplete_search_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "asciifolding"
          ]
        },
        "locality_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "asciifolding",
            "synonym_filter",
            "stop"
          ]
        }
      },
      "filter": {
        "edge_ngram_filter": {
          "type": "edge_ngram",
          "min_gram": 2,
          "max_gram": 20
        },
        "synonym_filter": {
          "type": "synonym",
          "synonyms": [
            "luxury,premium,deluxe,high-end",
            "gym,fitness,health club,gymnasium"
          ]
        }
      }
    }
}

# --- 2. Supply Mapping (Inventory) ---
#for something which could be matched fuzzily, it should be text, for exact matches, keyword
#for dual purpose, use multi fields
SUPPLY_MAPPING = {
    "properties": {
      "property_id": { "type": "keyword" },
      "customer_id": { "type": "keyword" },
      "title": { "type": "text", "analyzer": "fuzzy_search_analyzer" },
      "description": { "type": "text", "analyzer": "fuzzy_search_analyzer" },
      
      "locality": {
        "type": "text",
        "analyzer": "locality_analyzer",
        "fields": { "keyword": { "type": "keyword" } }
      },
      
      "property_type": { "type": "keyword" },
      "listing_type": { "type": "keyword" },
      "furnishing_status": { "type": "keyword" },
      "facing_direction": { "type": "keyword" },

      "price": { "type": "float" },
      "deposit": { "type": "float" }, 
      "bhk": { "type": "integer" },
      "bathrooms": { "type": "integer" },
      "area_sqft": { "type": "integer" },
      "age_of_building": { "type": "integer" },
      "lift_available": { "type": "boolean" },
      "floor_number": { "type": "integer" },
      "total_floors": { "type": "integer" },

      "listed_date": { "type": "date", "format": "yyyy-MM-dd" },
      
      "amenities": { "type": "keyword", "fields": { "text": { "type": "text", "analyzer": "fuzzy_search_analyzer" } } },
      "overlooking": { "type": "keyword", "fields": { "text": { "type": "text", "analyzer": "standard" } } },
      "additional_rooms": { "type": "keyword", "fields": { "text": { "type": "text", "analyzer": "standard" } } },
      
      "customer_name": { "type": "text" },
      "customer_email": { "type": "keyword" },
      "customer_phone": { "type": "keyword" },
      "customer_address": { "type": "text" },
      "customer_referred_by": { "type": "text" },
      "customer_additional_info": { "type": "text" },
      "text_content": {
        "type": "text",
        "analyzer": "fuzzy_search_analyzer",
        "copy_to": ["title", "description", "locality", "customer_name"]
      }
    }
  }

# --- 3. Demand Mapping (Requests) ---, veify this from popsi
DEMAND_MAPPING = {
    "properties": {
      "property_id": { "type": "keyword" }, # Used as the request ID
      "customer_id": { "type": "keyword" },
      "title": { "type": "text", "analyzer": "fuzzy_search_analyzer" },
      "description": { "type": "text", "analyzer": "fuzzy_search_analyzer" },
      
      "locality": {
        "type": "text",
        "analyzer": "locality_analyzer",
        "fields": { "keyword": { "type": "keyword" } }
      },
      
      "property_type": { "type": "keyword" },
      "listing_type": { "type": "keyword" },
      "furnishing_status": { "type": "keyword" },

      # Range Fields (Min/Max)
      "price_min": { "type": "float" },
      "price_max": { "type": "float" },
      "deposit_max": { "type": "float" }, 
      
      "bhk_min": { "type": "integer" },
      "bhk_max": { "type": "integer" },
      "area_sqft_min": { "type": "integer" },
      "area_sqft_max": { "type": "integer" },
      
      "bathrooms": { "type": "integer" },
      "lift_available": { "type": "boolean" },
      "listed_date": { "type": "date", "format": "yyyy-MM-dd" },
      "move_in_date": { "type": "date", "format": "yyyy-MM-dd" }, 
      "amenities": { "type": "keyword", "fields": { "text": { "type": "text", "analyzer": "fuzzy_search_analyzer" } } },
      "overlooking": { "type": "keyword", "fields": { "text": { "type": "text", "analyzer": "standard" } } },
      "additional_rooms": { "type": "keyword", "fields": { "text": { "type": "text", "analyzer": "standard" } } },
      
      "customer_name": { "type": "text" },
      "customer_email": { "type": "keyword" },
      "customer_phone": { "type": "keyword" },
      "customer_address": { "type": "text" },
      "customer_referred_by": { "type": "text" },
      "customer_additional_info": { "type": "text" },

      "text_content": {
        "type": "text",
        "analyzer": "fuzzy_search_analyzer",
        "copy_to": ["title", "description", "locality", "customer_name"]
      }
    }
  }

# --- 4. Initialization Logic ---

def create_index_if_not_exists(index_name: str, mapping: dict):
    """Creates a single index with the specified mapping if it does not exist."""
    print(f"Checking index: {index_name}...")
    try:
        if not client.indices.exists(index=index_name):
            print(f"Creating index: {index_name}...")
            
            body = {
                "settings": ANALYSIS_SETTINGS,
                "mappings": mapping
            }
            
            client.indices.create(index=index_name, body=body)
            print(f"Index {index_name} created successfully.")
        else:
            print(f"Index {index_name} already exists. Skipping creation.")

    except Exception as e:
        print(f"Error creating index {index_name}: {e}")
        print("Ensure OpenSearch is running and accessible.")

def initialize_opensearch():
    """Initializes both the supply and demand indices."""
    print("\n--- Starting OpenSearch Initialization ---")
    
    # Wait for OpenSearch to be available (useful when running with Docker Compose)
    max_retries = 5
    for i in range(max_retries):
        try:
            client.ping()
            print("OpenSearch connection successful.")
            break
        except Exception:
            print(f"Waiting for OpenSearch... Retry {i+1}/{max_retries}")
            time.sleep(5)
    else:
        print("FATAL: OpenSearch service is unreachable.")
        return

    # Create Supply Index
    create_index_if_not_exists(INDEX_SUPPLY, SUPPLY_MAPPING)

    # Create Demand Index
    create_index_if_not_exists(INDEX_DEMAND, DEMAND_MAPPING)
    
    print("--- OpenSearch Initialization Complete ---")

if __name__ == "__main__":
    # You can run this file directly to set up your OpenSearch indices
    initialize_opensearch()