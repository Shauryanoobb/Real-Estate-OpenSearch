"""
OpenSearch Recovery & Reindexing Utility

This script syncs data from PostgreSQL to OpenSearch indices.
Use cases:
- Recover from index deletion/corruption
- Apply new mapping changes
- Sync data after manual DB modifications
- Initial population of OpenSearch from existing data

Usage:
    python -m backend.core.recovery --help
    python -m backend.core.recovery --all
    python -m backend.core.recovery --supply --demand
    python -m backend.core.recovery --supply --recreate-index
    python -m backend.core.recovery --all --dry-run
"""

import sys
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from opensearchpy.helpers import bulk, BulkIndexError

# Import existing clients and models
from backend.core.database_client import SessionLocal
from backend.core.opensearch_client import client as opensearch_client
from backend.models.sql_property import SQLSupplyProperty, SQLDemandRequest
from backend.core.opensearch_init import (
    INDEX_SUPPLY,
    INDEX_DEMAND,
    SUPPLY_MAPPING,
    DEMAND_MAPPING,
    ANALYSIS_SETTINGS
)


class ReindexStats:
    """Track statistics during reindexing"""
    def __init__(self):
        self.total_records = 0
        self.indexed_records = 0
        self.failed_records = 0
        self.errors: List[Dict[str, Any]] = []
        self.start_time = datetime.now()

    def add_success(self, count: int):
        self.indexed_records += count

    def add_failure(self, doc_id: str, error: str):
        self.failed_records += 1
        self.errors.append({"id": doc_id, "error": str(error)})

    def print_summary(self, index_name: str):
        duration = (datetime.now() - self.start_time).total_seconds()
        print(f"\n{'='*60}")
        print(f"Reindexing Summary for: {index_name}")
        print(f"{'='*60}")
        print(f"Total Records:    {self.total_records}")
        print(f"Successfully Indexed: {self.indexed_records}")
        print(f"Failed:           {self.failed_records}")
        print(f"Duration:         {duration:.2f} seconds")
        print(f"{'='*60}")

        if self.errors:
            print(f"\n⚠️  First 10 errors:")
            for error in self.errors[:10]:
                print(f"  - ID {error['id']}: {error['error']}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more errors")


def clean_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Clean document data for OpenSearch indexing.
    - Remove None values (OpenSearch can handle missing fields)
    - Handle empty arrays
    - Ensure proper data types
    """
    cleaned = {}
    for key, value in doc.items():
        # Skip SQLAlchemy internal state
        if key.startswith('_'):
            continue

        # Skip None values
        if value is None:
            continue

        # Handle arrays - keep empty arrays as they're valid
        if isinstance(value, list):
            cleaned[key] = value
        else:
            cleaned[key] = value

    return cleaned


def generate_bulk_actions(
    records: List[Any],
    index_name: str,
    id_field: str = "property_id"
) -> List[Dict[str, Any]]:
    """
    Generate bulk action dicts for OpenSearch bulk API.

    Args:
        records: List of SQLAlchemy model instances
        index_name: Target index name
        id_field: Field name to use as document ID (auto-detected if not provided)

    Returns:
        List of bulk action dictionaries
    """
    actions = []
    for record in records:
        doc = record.to_dict()

        # Smart ID detection: try property_id, then request_id, then id
        doc_id = doc.get(id_field) or doc.get('property_id') or doc.get('request_id') or doc.get('id')

        if not doc_id:
            print(f"⚠️  Warning: Record missing ID field, skipping: {doc}")
            continue

        # Clean document
        cleaned_doc = clean_document(doc)

        # Create bulk action
        action = {
            "_index": index_name,
            "_id": doc_id,
            "_source": cleaned_doc
        }
        actions.append(action)

    return actions


def reindex_table(
    db_session: Session,
    model_class: type,
    index_name: str,
    batch_size: int = 500,
    dry_run: bool = False,
    recreate_index: bool = False,
    mapping: Optional[Dict] = None
) -> ReindexStats:
    """
    Reindex a single table from PostgreSQL to OpenSearch.

    Args:
        db_session: SQLAlchemy database session
        model_class: SQLAlchemy model class (e.g., SQLSupplyProperty)
        index_name: Target OpenSearch index name
        batch_size: Number of documents to index per batch
        dry_run: If True, don't actually index (just print what would happen)
        recreate_index: If True, delete and recreate the index first
        mapping: Index mapping to use if recreating

    Returns:
        ReindexStats object with statistics
    """
    stats = ReindexStats()

    print(f"\n{'─'*60}")
    print(f"🔄 Reindexing: {model_class.__tablename__} → {index_name}")
    print(f"{'─'*60}")

    # Recreate index if requested
    if recreate_index and not dry_run:
        try:
            if opensearch_client.indices.exists(index=index_name):
                print(f"🗑️  Deleting existing index: {index_name}")
                opensearch_client.indices.delete(index=index_name)
                print(f"✓ Index deleted")

            print(f"🔨 Creating new index: {index_name}")
            body = {
                "settings": ANALYSIS_SETTINGS,
                "mappings": mapping
            }
            opensearch_client.indices.create(index=index_name, body=body)
            print(f"✓ Index created with new mapping")
        except Exception as e:
            print(f"❌ Error recreating index: {e}")
            return stats

    # Count total records
    try:
        total_count = db_session.query(model_class).count()
        stats.total_records = total_count
        print(f"📊 Found {total_count} records in database")

        if total_count == 0:
            print("⚠️  No records to index")
            return stats
    except Exception as e:
        print(f"❌ Error counting records: {e}")
        return stats

    # Process in batches
    offset = 0
    batch_num = 0

    while offset < total_count:
        batch_num += 1
        print(f"\n📦 Processing batch {batch_num} (records {offset} to {offset + batch_size})...")

        try:
            # Fetch batch from database
            records = db_session.query(model_class).offset(offset).limit(batch_size).all()

            if not records:
                break

            # Generate bulk actions
            actions = generate_bulk_actions(records, index_name)

            if dry_run:
                print(f"   [DRY RUN] Would index {len(actions)} documents")
                stats.add_success(len(actions))
            else:
                # Execute bulk indexing
                try:
                    success, failed = bulk(
                        opensearch_client,
                        actions,
                        stats_only=True,
                        raise_on_error=False
                    )
                    stats.add_success(success)

                    if failed:
                        print(f"   ⚠️  {failed} documents failed in this batch")
                        stats.failed_records += failed
                    else:
                        print(f"   ✓ Indexed {success} documents")

                except BulkIndexError as e:
                    print(f"   ❌ Bulk indexing error: {e}")
                    for error_item in e.errors:
                        doc_id = error_item.get('index', {}).get('_id', 'unknown')
                        error_msg = error_item.get('index', {}).get('error', 'unknown error')
                        stats.add_failure(doc_id, error_msg)

                except Exception as e:
                    print(f"   ❌ Unexpected error: {e}")
                    stats.failed_records += len(actions)

            offset += batch_size

        except Exception as e:
            print(f"   ❌ Error processing batch: {e}")
            offset += batch_size
            continue

    # Refresh index to make documents immediately visible for verification
    if not dry_run and stats.indexed_records > 0:
        try:
            print(f"\n🔄 Refreshing index to make documents visible...")
            opensearch_client.indices.refresh(index=index_name)
            print(f"✓ Index refreshed")
        except Exception as e:
            print(f"⚠️  Warning: Failed to refresh index: {e}")

    return stats


def verify_sync(
    db_session: Session,
    model_class: type,
    index_name: str
) -> bool:
    """
    Verify that PostgreSQL and OpenSearch have the same record count.

    Returns:
        True if counts match, False otherwise
    """
    try:
        # Count in PostgreSQL
        db_count = db_session.query(model_class).count()

        # Count in OpenSearch
        os_response = opensearch_client.count(index=index_name)
        os_count = os_response.get('count', 0)

        print(f"\n🔍 Verification for {index_name}:")
        print(f"   PostgreSQL: {db_count} records")
        print(f"   OpenSearch: {os_count} records")

        if db_count == os_count:
            print(f"   ✓ Counts match!")
            return True
        else:
            print(f"   ⚠️  Counts differ by {abs(db_count - os_count)} records")
            return False

    except Exception as e:
        print(f"   ❌ Error during verification: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Reindex PostgreSQL data to OpenSearch",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Reindex all indices
  python -m backend.core.recovery --all

  # Reindex supply properties only
  python -m backend.core.recovery --supply

  # Reindex with index recreation (fresh mappings)
  python -m backend.core.recovery --all --recreate-index

  # Dry run (preview without changes)
  python -m backend.core.recovery --all --dry-run

  # Custom batch size
  python -m backend.core.recovery --supply --batch-size 1000

  # Skip verification
  python -m backend.core.recovery --all --no-verify
        """
    )

    # Index selection
    parser.add_argument(
        "--all",
        action="store_true",
        help="Reindex all indices (supply and demand)"
    )
    parser.add_argument(
        "--supply",
        action="store_true",
        help="Reindex supply_properties index"
    )
    parser.add_argument(
        "--demand",
        action="store_true",
        help="Reindex demand_requests index"
    )

    # Options
    parser.add_argument(
        "--recreate-index",
        action="store_true",
        help="Delete and recreate indices before reindexing (applies fresh mappings)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of documents per batch (default: 500)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would happen without making changes"
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip verification step after reindexing"
    )

    args = parser.parse_args()

    # Determine which indices to reindex
    reindex_supply = args.supply or args.all
    reindex_demand = args.demand or args.all

    if not (reindex_supply or reindex_demand):
        parser.print_help()
        print("\n❌ Error: Must specify --all, --supply, or --demand")
        sys.exit(1)

    # Print configuration
    print("╔" + "═"*58 + "╗")
    print("║" + " "*15 + "OpenSearch Recovery Tool" + " "*19 + "║")
    print("╚" + "═"*58 + "╝")
    print(f"\nConfiguration:")
    print(f"  • Batch Size: {args.batch_size}")
    print(f"  • Recreate Indices: {args.recreate_index}")
    print(f"  • Dry Run: {args.dry_run}")
    print(f"  • Verify After: {not args.no_verify}")
    print(f"\nTarget Indices:")
    if reindex_supply:
        print(f"  ✓ Supply Properties")
    if reindex_demand:
        print(f"  ✓ Demand Requests")

    if args.dry_run:
        print(f"\n⚠️  DRY RUN MODE - No changes will be made")

    # Confirm if recreating indices
    if args.recreate_index and not args.dry_run:
        print(f"\n⚠️  WARNING: This will DELETE and RECREATE the indices!")
        response = input("Are you sure? Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("Aborted.")
            sys.exit(0)

    # Create database session
    db_session = SessionLocal()

    try:
        # Test OpenSearch connection
        if not opensearch_client.ping():
            print("❌ Error: Cannot connect to OpenSearch")
            sys.exit(1)
        print("\n✓ Connected to OpenSearch")

        # Reindex supply properties
        if reindex_supply:
            supply_stats = reindex_table(
                db_session=db_session,
                model_class=SQLSupplyProperty,
                index_name=INDEX_SUPPLY,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                recreate_index=args.recreate_index,
                mapping=SUPPLY_MAPPING
            )
            supply_stats.print_summary(INDEX_SUPPLY)

            if not args.dry_run and not args.no_verify:
                verify_sync(db_session, SQLSupplyProperty, INDEX_SUPPLY)

        # Reindex demand requests
        if reindex_demand:
            demand_stats = reindex_table(
                db_session=db_session,
                model_class=SQLDemandRequest,
                index_name=INDEX_DEMAND,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
                recreate_index=args.recreate_index,
                mapping=DEMAND_MAPPING
            )
            demand_stats.print_summary(INDEX_DEMAND)

            if not args.dry_run and not args.no_verify:
                verify_sync(db_session, SQLDemandRequest, INDEX_DEMAND)

        print("\n✅ Recovery complete!")

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)

    finally:
        db_session.close()


if __name__ == "__main__":
    main()