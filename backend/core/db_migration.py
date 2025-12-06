"""
Database Migration Utility

Handles schema changes and data migrations for PostgreSQL and OpenSearch.
For low-volume datasets, uses simple ALTER TABLE approach.

Usage:
    python -m backend.core.db_migration add-column supply_properties new_field String
    python -m backend.core.db_migration add-column demand_requests price_verified Boolean
    python -m backend.core.db_migration sync-schema
    python -m backend.core.db_migration list-columns supply_properties

Use Cases:
    - Add new optional fields to existing tables
    - Sync schema changes from PostgreSQL to OpenSearch
    - View current table structure
    - Handle backward-compatible schema evolution
"""

import sys
import argparse
from typing import Optional, Dict, Any
from sqlalchemy import inspect, text

from backend.core.database_client import engine
from backend.core.opensearch_client import client as opensearch_client
from backend.core.opensearch_init import INDEX_SUPPLY, INDEX_DEMAND


# Type mapping for PostgreSQL
POSTGRES_TYPE_MAP = {
    'String': 'VARCHAR',
    'Integer': 'INTEGER',
    'Float': 'FLOAT',
    'Boolean': 'BOOLEAN',
    'Date': 'DATE',
    'DateTime': 'TIMESTAMP',
    'Array[String]': 'VARCHAR[]',
    'Array[Integer]': 'INTEGER[]',
}

# Type mapping for OpenSearch
OPENSEARCH_TYPE_MAP = {
    'String': 'text',
    'Integer': 'integer',
    'Float': 'float',
    'Boolean': 'boolean',
    'Date': 'date',
    'DateTime': 'date',
    'Array[String]': 'keyword',
    'Array[Integer]': 'integer',
}


def list_table_columns(table_name: str):
    """
    List all columns in a PostgreSQL table with their types.
    """
    print(f"\n📋 Columns in '{table_name}':\n")
    print(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10}")
    print("-" * 60)

    try:
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)

        for col in columns:
            col_name = col['name']
            col_type = str(col['type'])
            nullable = "Yes" if col['nullable'] else "No"
            print(f"{col_name:<30} {col_type:<20} {nullable:<10}")

        print(f"\nTotal columns: {len(columns)}")

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

    return True


def add_column_to_postgres(
    table_name: str,
    column_name: str,
    column_type: str,
    nullable: bool = True,
    default_value: Optional[str] = None
) -> bool:
    """
    Add a new column to a PostgreSQL table using ALTER TABLE.

    Args:
        table_name: Name of the table
        column_name: Name of the new column
        column_type: Type from POSTGRES_TYPE_MAP
        nullable: Whether column accepts NULL values
        default_value: Default value for existing rows

    Returns:
        True if successful, False otherwise
    """
    print(f"\n🔧 Adding column to PostgreSQL...")
    print(f"   Table: {table_name}")
    print(f"   Column: {column_name}")
    print(f"   Type: {column_type}")

    # Map type
    pg_type = POSTGRES_TYPE_MAP.get(column_type, column_type)

    # Build ALTER TABLE statement
    null_constraint = "" if nullable else "NOT NULL"
    default_clause = f"DEFAULT {default_value}" if default_value else ""

    sql = f"""
    ALTER TABLE {table_name}
    ADD COLUMN IF NOT EXISTS {column_name} {pg_type} {null_constraint} {default_clause};
    """

    try:
        with engine.connect() as conn:
            # Use execution_options for correct autocommit behavior
            conn.execute(text(sql))
            conn.commit()

        print(f"✅ Column '{column_name}' added to '{table_name}'")
        return True

    except Exception as e:
        print(f"❌ Error adding column: {e}")
        return False


def add_field_to_opensearch_mapping(
    index_name: str,
    field_name: str,
    field_type: str
) -> bool:
    """
    Add a new field to OpenSearch index mapping using PUT mapping API.

    OpenSearch allows adding new fields to existing indices dynamically.

    Args:
        index_name: Name of the index
        field_name: Name of the new field
        field_type: Type from OPENSEARCH_TYPE_MAP

    Returns:
        True if successful, False otherwise
    """
    print(f"\n🔧 Adding field to OpenSearch...")
    print(f"   Index: {index_name}")
    print(f"   Field: {field_name}")
    print(f"   Type: {field_type}")

    # Map type
    os_type = OPENSEARCH_TYPE_MAP.get(field_type, 'text')

    # Build mapping
    mapping = {
        "properties": {
            field_name: {
                "type": os_type
            }
        }
    }

    try:
        # Check if index exists
        if not opensearch_client.indices.exists(index=index_name):
            print(f"⚠️  Index '{index_name}' does not exist")
            return False

        # Update mapping
        response = opensearch_client.indices.put_mapping(
            index=index_name,
            body=mapping
        )

        if response.get('acknowledged'):
            print(f"✅ Field '{field_name}' added to '{index_name}' mapping")
            return True
        else:
            print(f"⚠️  Mapping update not acknowledged: {response}")
            return False

    except Exception as e:
        print(f"❌ Error updating OpenSearch mapping: {e}")
        return False


def get_opensearch_mapping(index_name: str) -> Optional[Dict[str, Any]]:
    """Get current mapping for an OpenSearch index"""
    try:
        if not opensearch_client.indices.exists(index=index_name):
            print(f"⚠️  Index '{index_name}' does not exist")
            return None

        response = opensearch_client.indices.get_mapping(index=index_name)
        return response.get(index_name, {}).get('mappings', {})

    except Exception as e:
        print(f"❌ Error fetching mapping: {e}")
        return None


def list_opensearch_fields(index_name: str):
    """List all fields in an OpenSearch index mapping"""
    print(f"\n📋 Fields in OpenSearch index '{index_name}':\n")

    mapping = get_opensearch_mapping(index_name)
    if not mapping:
        return False

    properties = mapping.get('properties', {})

    print(f"{'Field Name':<30} {'Type':<20}")
    print("-" * 50)

    for field_name, field_config in sorted(properties.items()):
        field_type = field_config.get('type', 'unknown')
        print(f"{field_name:<30} {field_type:<20}")

    print(f"\nTotal fields: {len(properties)}")
    return True


def sync_schema_from_postgres_to_opensearch(table_name: str, index_name: str) -> bool:
    """
    Sync schema from PostgreSQL to OpenSearch by comparing columns and fields.
    Adds any missing fields to OpenSearch.

    Args:
        table_name: PostgreSQL table name
        index_name: OpenSearch index name

    Returns:
        True if successful, False otherwise
    """
    print(f"\n🔄 Syncing schema: {table_name} → {index_name}")

    try:
        # Get PostgreSQL columns
        inspector = inspect(engine)
        pg_columns = inspector.get_columns(table_name)

        # Get OpenSearch mapping
        os_mapping = get_opensearch_mapping(index_name)
        if not os_mapping:
            return False

        os_fields = set(os_mapping.get('properties', {}).keys())

        # Find missing fields in OpenSearch
        missing_fields = []
        for col in pg_columns:
            col_name = col['name']
            if col_name not in os_fields and col_name != '_sa_instance_state':
                missing_fields.append(col)

        if not missing_fields:
            print("✓ Schema already in sync - no missing fields")
            return True

        print(f"\n📋 Found {len(missing_fields)} missing field(s) in OpenSearch:")
        for col in missing_fields:
            print(f"   - {col['name']} ({col['type']})")

        # Add missing fields
        for col in missing_fields:
            col_name = col['name']
            col_type_str = str(col['type'])

            # Infer OpenSearch type from PostgreSQL type
            if 'VARCHAR' in col_type_str or 'TEXT' in col_type_str:
                os_type = 'text'
            elif 'INTEGER' in col_type_str:
                os_type = 'integer'
            elif 'FLOAT' in col_type_str or 'NUMERIC' in col_type_str:
                os_type = 'float'
            elif 'BOOLEAN' in col_type_str:
                os_type = 'boolean'
            elif 'DATE' in col_type_str:
                os_type = 'date'
            elif 'ARRAY' in col_type_str:
                os_type = 'keyword'  # Default arrays to keyword
            else:
                os_type = 'text'  # Default to text

            # Add field to OpenSearch
            mapping = {
                "properties": {
                    col_name: {"type": os_type}
                }
            }

            print(f"\n   Adding '{col_name}' as '{os_type}'...")
            opensearch_client.indices.put_mapping(
                index=index_name,
                body=mapping
            )

        print(f"\n✅ Schema sync complete!")
        return True

    except Exception as e:
        print(f"❌ Error syncing schema: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Database migration and schema management utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List columns in a PostgreSQL table
  python -m backend.core.db_migration list-columns supply_properties

  # List fields in an OpenSearch index
  python -m backend.core.db_migration list-fields supply_properties

  # Add a new optional column to PostgreSQL
  python -m backend.core.db_migration add-column supply_properties verified Boolean

  # Add a new field to OpenSearch
  python -m backend.core.db_migration add-field supply_properties verified Boolean

  # Add column to both PostgreSQL and OpenSearch
  python -m backend.core.db_migration add-column supply_properties verified Boolean --sync-opensearch

  # Sync schema from PostgreSQL to OpenSearch (add missing fields)
  python -m backend.core.db_migration sync-schema supply_properties supply_properties
  python -m backend.core.db_migration sync-schema demand_requests demand_requests

Supported Types:
  PostgreSQL: String, Integer, Float, Boolean, Date, DateTime, Array[String], Array[Integer]
  OpenSearch: String, Integer, Float, Boolean, Date, DateTime, Array[String], Array[Integer]
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # List columns command
    list_cols_parser = subparsers.add_parser('list-columns', help='List all columns in a PostgreSQL table')
    list_cols_parser.add_argument('table_name', help='Table name (e.g., supply_properties)')

    # List fields command
    list_fields_parser = subparsers.add_parser('list-fields', help='List all fields in an OpenSearch index')
    list_fields_parser.add_argument('index_name', help='Index name (e.g., supply_properties)')

    # Add column command
    add_col_parser = subparsers.add_parser('add-column', help='Add a new column to PostgreSQL table')
    add_col_parser.add_argument('table_name', help='Table name')
    add_col_parser.add_argument('column_name', help='New column name')
    add_col_parser.add_argument('column_type', help='Column type (String, Integer, Float, Boolean, etc.)')
    add_col_parser.add_argument('--nullable', action='store_true', default=True, help='Allow NULL values (default: True)')
    add_col_parser.add_argument('--default', help='Default value for existing rows')
    add_col_parser.add_argument('--sync-opensearch', action='store_true', help='Also add field to OpenSearch')

    # Add field command
    add_field_parser = subparsers.add_parser('add-field', help='Add a new field to OpenSearch index')
    add_field_parser.add_argument('index_name', help='Index name')
    add_field_parser.add_argument('field_name', help='New field name')
    add_field_parser.add_argument('field_type', help='Field type (String, Integer, Float, Boolean, etc.)')

    # Sync schema command
    sync_parser = subparsers.add_parser('sync-schema', help='Sync schema from PostgreSQL to OpenSearch')
    sync_parser.add_argument('table_name', help='PostgreSQL table name')
    sync_parser.add_argument('index_name', help='OpenSearch index name')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute command
    if args.command == 'list-columns':
        success = list_table_columns(args.table_name)

    elif args.command == 'list-fields':
        success = list_opensearch_fields(args.index_name)

    elif args.command == 'add-column':
        # Add to PostgreSQL
        success = add_column_to_postgres(
            args.table_name,
            args.column_name,
            args.column_type,
            args.nullable,
            args.default
        )

        # Also add to OpenSearch if requested
        if success and args.sync_opensearch:
            # Determine index name from table name
            if args.table_name == 'supply_properties':
                index_name = INDEX_SUPPLY
            elif args.table_name == 'demand_requests':
                index_name = INDEX_DEMAND
            else:
                index_name = args.table_name

            add_field_to_opensearch_mapping(
                index_name,
                args.column_name,
                args.column_type
            )

    elif args.command == 'add-field':
        success = add_field_to_opensearch_mapping(
            args.index_name,
            args.field_name,
            args.field_type
        )

    elif args.command == 'sync-schema':
        success = sync_schema_from_postgres_to_opensearch(
            args.table_name,
            args.index_name
        )

    else:
        parser.print_help()
        success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
