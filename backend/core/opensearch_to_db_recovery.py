"""
Database Recovery from OpenSearch

Recovers PostgreSQL data from OpenSearch indices when database is lost or corrupted.
This treats OpenSearch as a backup source to restore PostgreSQL tables.

Usage:
    python -m backend.core.db_recovery_from_opensearch --help
    python -m backend.core.db_recovery_from_opensearch --all
    python -m backend.core.db_recovery_from_opensearch --supply --demand
    python -m backend.core.db_recovery_from_opensearch --all --dry-run

Use Cases:
    - PostgreSQL database deleted or corrupted
    - Need to restore specific tables
    - Recover from catastrophic database failure
"""

import sys
import argparse
from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy import inspect
from sqlalchemy.orm import Session

# Import existing clients and models
from backend.core.database_client import SessionLocal, engine
from backend.core.opensearch_client import client as opensearch_client
from backend.models.sql_property import Base, SQLSupplyProperty, SQLDemandRequest
from backend.core.opensearch_init import INDEX_SUPPLY, INDEX_DEMAND


class RecoveryStats:
    """Track statistics during recovery"""
    def __init__(self):
        self.total_records = 0
        self.recovered_records = 0
        self.failed_records = 0
        self.errors: List[Dict[str, Any]] = []
        self.start_time = datetime.now()

    def add_success(self):
        self.recovered_records += 1

    def add_failure(self, doc_id: str, error: str):
        self.failed_records += 1
        self.errors.append({"id": doc_id, "error": str(error)})

    def print_summary(self, table_name: str):
        duration = (datetime.now() - self.start_time).total_seconds()
        print(f"\n{'='*60}")
        print(f"Recovery Summary for: {table_name}")
        print(f"{'='*60}")
        print(f"Total Records:    {self.total_records}")
        print(f"Recovered:        {self.recovered_records}")
        print(f"Failed:           {self.failed_records}")
        print(f"Duration:         {duration:.2f} seconds")
        print(f"{'='*60}")

        if self.errors:
            print(f"\n⚠️  First 10 errors:")
            for error in self.errors[:10]:
                print(f"  - ID {error['id']}: {error['error']}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more errors")


def fetch_all_from_index(index_name: str, batch_size: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch all documents from an OpenSearch index using scroll API.

    Args:
        index_name: Index to fetch from
        batch_size: Documents per scroll batch

    Returns:
        List of all documents
    """
    try:
        # Check if index exists
        if not opensearch_client.indices.exists(index=index_name):
            print(f"⚠️  Index {index_name} does not exist")
            return []

        # Get total count
        count_response = opensearch_client.count(index=index_name)
        total = count_response.get('count', 0)
        print(f"📊 Found {total} documents in {index_name}")

        if total == 0:
            return []

        # Use scroll API for efficient bulk retrieval
        documents = []
        scroll_response = opensearch_client.search(
            index=index_name,
            body={
                "query": {"match_all": {}},
                "size": batch_size
            },
            scroll="2m"  # Keep scroll context for 2 minutes
        )

        scroll_id = scroll_response['_scroll_id']
        hits = scroll_response['hits']['hits']

        while hits:
            for hit in hits:
                doc = hit['_source']
                doc['_id'] = hit['_id']  # Preserve document ID
                documents.append(doc)

            # Get next batch
            scroll_response = opensearch_client.scroll(
                scroll_id=scroll_id,
                scroll="2m"
            )
            hits = scroll_response['hits']['hits']

        # Clear scroll context
        opensearch_client.clear_scroll(scroll_id=scroll_id)

        print(f"✓ Fetched {len(documents)} documents from {index_name}")
        return documents

    except Exception as e:
        print(f"❌ Error fetching from {index_name}: {e}")
        return []


def map_opensearch_to_sql(
    os_doc: Dict[str, Any],
    model_class: type
) -> Dict[str, Any]:
    """
    Map OpenSearch document fields to SQLAlchemy model fields.

    Handles field name differences:
    - property_id → id (for supply)
    - request_id → id (for demand)

    Args:
        os_doc: OpenSearch document
        model_class: SQLAlchemy model class

    Returns:
        Dict with mapped fields
    """
    mapped = {}

    # Get model columns
    inspector = inspect(model_class)
    columns = {col.key for col in inspector.columns}

    # Map property_id/request_id back to id
    if 'property_id' in os_doc:
        mapped['id'] = os_doc['property_id']
    elif 'request_id' in os_doc:
        mapped['id'] = os_doc['request_id']
    elif '_id' in os_doc:
        mapped['id'] = os_doc['_id']

    # Copy all other fields that exist in the model
    for key, value in os_doc.items():
        if key in ['_id', 'property_id', 'request_id']:
            continue

        if key in columns:
            # Handle None/null values
            if value is not None:
                mapped[key] = value

    return mapped


def recover_table(
    db_session: Session,
    model_class: type,
    index_name: str,
    dry_run: bool = False,
    truncate_first: bool = False
) -> RecoveryStats:
    """
    Recover a PostgreSQL table from an OpenSearch index.

    Args:
        db_session: SQLAlchemy database session
        model_class: SQLAlchemy model class
        index_name: Source OpenSearch index
        dry_run: If True, don't actually write to database
        truncate_first: If True, delete all existing records first

    Returns:
        RecoveryStats object
    """
    stats = RecoveryStats()

    print(f"\n{'─'*60}")
    print(f"🔄 Recovering: {index_name} → {model_class.__tablename__}")
    print(f"{'─'*60}")

    # Truncate table if requested
    if truncate_first and not dry_run:
        print(f"🗑️  Truncating existing records in {model_class.__tablename__}...")
        try:
            db_session.query(model_class).delete()
            db_session.commit()
            print(f"✓ Table truncated")
        except Exception as e:
            print(f"❌ Error truncating table: {e}")
            db_session.rollback()
            return stats

    # Fetch all documents from OpenSearch
    documents = fetch_all_from_index(index_name)
    stats.total_records = len(documents)

    if not documents:
        print("⚠️  No documents to recover")
        return stats

    # Process each document
    print(f"\n📝 Processing {len(documents)} documents...")

    for i, os_doc in enumerate(documents, 1):
        try:
            # Map OpenSearch fields to SQL fields
            sql_data = map_opensearch_to_sql(os_doc, model_class)

            if not sql_data.get('id'):
                print(f"   ⚠️  Document {i} missing ID, skipping")
                stats.add_failure("unknown", "Missing ID field")
                continue

            if dry_run:
                print(f"   [{i}/{len(documents)}] [DRY RUN] Would insert ID: {sql_data['id']}")
                stats.add_success()
            else:
                # Check if record already exists
                existing = db_session.query(model_class).filter_by(id=sql_data['id']).first()

                if existing:
                    # Update existing record
                    for key, value in sql_data.items():
                        setattr(existing, key, value)
                    action = "Updated"
                else:
                    # Insert new record
                    new_record = model_class(**sql_data)
                    db_session.add(new_record)
                    action = "Inserted"

                db_session.commit()
                stats.add_success()

                if i % 50 == 0:  # Progress update every 50 records
                    print(f"   [{i}/{len(documents)}] {action} ID: {sql_data['id']}")

        except Exception as e:
            doc_id = os_doc.get('property_id') or os_doc.get('request_id') or os_doc.get('_id', 'unknown')
            print(f"   ❌ Error processing document {doc_id}: {e}")
            stats.add_failure(doc_id, str(e))
            if not dry_run:
                db_session.rollback()

    print(f"✓ Processing complete")
    return stats


def verify_recovery(
    db_session: Session,
    model_class: type,
    index_name: str
) -> bool:
    """
    Verify recovery by comparing record counts.

    Returns:
        True if counts match, False otherwise
    """
    try:
        # Count in PostgreSQL
        db_count = db_session.query(model_class).count()

        # Count in OpenSearch
        os_response = opensearch_client.count(index=index_name)
        os_count = os_response.get('count', 0)

        print(f"\n🔍 Verification for {model_class.__tablename__}:")
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


def ensure_tables_exist():
    """Create database tables if they don't exist"""
    try:
        print("🔧 Ensuring database tables exist...")
        Base.metadata.create_all(bind=engine)
        print("✓ Tables ready")
        return True
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Recover PostgreSQL database from OpenSearch indices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Recover all tables from OpenSearch
  python -m backend.core.db_recovery_from_opensearch --all

  # Recover only supply properties
  python -m backend.core.db_recovery_from_opensearch --supply

  # Dry run (preview without changes)
  python -m backend.core.db_recovery_from_opensearch --all --dry-run

  # Truncate tables before recovery (clean slate)
  python -m backend.core.db_recovery_from_opensearch --all --truncate

  # Skip verification
  python -m backend.core.db_recovery_from_opensearch --all --no-verify
        """
    )

    # Table selection
    parser.add_argument(
        "--all",
        action="store_true",
        help="Recover all tables (supply and demand)"
    )
    parser.add_argument(
        "--supply",
        action="store_true",
        help="Recover supply_properties table"
    )
    parser.add_argument(
        "--demand",
        action="store_true",
        help="Recover demand_requests table"
    )

    # Options
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete all existing records before recovery"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would happen without making changes"
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip verification step after recovery"
    )

    args = parser.parse_args()

    # Determine which tables to recover
    recover_supply = args.supply or args.all
    recover_demand = args.demand or args.all

    if not (recover_supply or recover_demand):
        parser.print_help()
        print("\n❌ Error: Must specify --all, --supply, or --demand")
        sys.exit(1)

    # Print configuration
    print("╔" + "═"*58 + "╗")
    print("║" + " "*12 + "Database Recovery from OpenSearch" + " "*13 + "║")
    print("╚" + "═"*58 + "╝")
    print(f"\nConfiguration:")
    print(f"  • Truncate First: {args.truncate}")
    print(f"  • Dry Run: {args.dry_run}")
    print(f"  • Verify After: {not args.no_verify}")
    print(f"\nTarget Tables:")
    if recover_supply:
        print(f"  ✓ Supply Properties")
    if recover_demand:
        print(f"  ✓ Demand Requests")

    if args.dry_run:
        print(f"\n⚠️  DRY RUN MODE - No changes will be made")

    # Confirm if truncating
    if args.truncate and not args.dry_run:
        print(f"\n⚠️  WARNING: This will DELETE all existing data before recovery!")
        response = input("Are you sure? Type 'yes' to continue: ")
        if response.lower() != 'yes':
            print("Aborted.")
            sys.exit(0)

    # Test connections
    try:
        if not opensearch_client.ping():
            print("❌ Error: Cannot connect to OpenSearch")
            sys.exit(1)
        print("\n✓ Connected to OpenSearch")
    except Exception as e:
        print(f"❌ Error connecting to OpenSearch: {e}")
        sys.exit(1)

    # Ensure database tables exist
    if not ensure_tables_exist():
        sys.exit(1)

    # Create database session
    db_session = SessionLocal()

    try:
        # Recover supply properties
        if recover_supply:
            supply_stats = recover_table(
                db_session=db_session,
                model_class=SQLSupplyProperty,
                index_name=INDEX_SUPPLY,
                dry_run=args.dry_run,
                truncate_first=args.truncate
            )
            supply_stats.print_summary("supply_properties")

            if not args.dry_run and not args.no_verify:
                verify_recovery(db_session, SQLSupplyProperty, INDEX_SUPPLY)

        # Recover demand requests
        if recover_demand:
            demand_stats = recover_table(
                db_session=db_session,
                model_class=SQLDemandRequest,
                index_name=INDEX_DEMAND,
                dry_run=args.dry_run,
                truncate_first=args.truncate
            )
            demand_stats.print_summary("demand_requests")

            if not args.dry_run and not args.no_verify:
                verify_recovery(db_session, SQLDemandRequest, INDEX_DEMAND)

        print("\n✅ Recovery complete!")

    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)

    finally:
        db_session.close()


if __name__ == "__main__":
    main()
