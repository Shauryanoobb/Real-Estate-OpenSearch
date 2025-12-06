"""
PostgreSQL Backup Utility

Creates SQL dump backups of the PostgreSQL database.
For low-volume datasets (<1000 records), this is fast, simple, and effective.

Usage:
    python -m backend.core.db_backup
    python -m backend.core.db_backup --output /path/to/backup.sql
    python -m backend.core.db_backup --compress

Schedule with cron:
    # Daily backup at 2 AM
    0 2 * * * cd /path/to/project && python -m backend.core.db_backup --compress
"""

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path
import gzip
import shutil
import os


# Database configuration (matches database_client.py)
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "realestate_db"
DB_USER = "user"
DB_PASSWORD = "password"  # Will use PGPASSWORD env var

# Default backup directory
DEFAULT_BACKUP_DIR = Path(__file__).parent.parent.parent / "backups"


def find_postgres_binary(binary_name: str) -> str:
    """
    Find PostgreSQL binary (pg_dump, psql) in system PATH or common locations.

    Args:
        binary_name: Name of binary to find (e.g., 'pg_dump', 'psql')

    Returns:
        Full path to binary

    Raises:
        FileNotFoundError: If binary cannot be found
    """
    # Try to find in PATH first
    binary_path = shutil.which(binary_name)
    if binary_path:
        return binary_path

    # Common installation locations (especially for Homebrew on macOS)
    common_locations = [
        f"/opt/homebrew/bin/{binary_name}",           # Homebrew ARM (M1/M2/M3 Macs)
        f"/usr/local/bin/{binary_name}",              # Homebrew Intel Macs
        f"/opt/homebrew/opt/postgresql@16/bin/{binary_name}",  # Homebrew PostgreSQL 16
        f"/opt/homebrew/opt/postgresql@15/bin/{binary_name}",  # Homebrew PostgreSQL 15
        f"/usr/local/opt/postgresql@16/bin/{binary_name}",     # Intel PostgreSQL 16
        f"/usr/bin/{binary_name}",                    # System installation
        f"/Library/PostgreSQL/16/bin/{binary_name}",  # PostgreSQL.org installer
    ]

    for location in common_locations:
        if Path(location).exists():
            return location

    # Not found anywhere
    raise FileNotFoundError(
        f"{binary_name} not found. Please install PostgreSQL client tools:\n"
        f"  - macOS: brew install postgresql\n"
        f"  - Ubuntu: apt-get install postgresql-client\n"
        f"  - Windows: Download from postgresql.org"
    )


def create_backup_filename(compress: bool = False) -> str:
    """Generate timestamped backup filename"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    extension = ".sql.gz" if compress else ".sql"
    return f"realestate_backup_{timestamp}{extension}"


def run_pg_dump(output_path: Path, compress: bool = False) -> bool:
    """
    Execute pg_dump to create database backup.

    Args:
        output_path: Path where backup should be saved
        compress: If True, compress with gzip

    Returns:
        True if successful, False otherwise
    """
    print(f"🔄 Creating database backup...")
    print(f"   Database: {DB_NAME}")
    print(f"   Output: {output_path}")

    # Find pg_dump binary
    try:
        pg_dump_path = find_postgres_binary("pg_dump")
        print(f"   Using: {pg_dump_path}")
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return False

    # Prepare pg_dump command
    cmd = [
        pg_dump_path,
        "-h", DB_HOST,
        "-p", DB_PORT,
        "-U", DB_USER,
        "-d", DB_NAME,
        "--clean",              # Include DROP statements
        "--if-exists",          # Use IF EXISTS for drops
        "--create",             # Include CREATE DATABASE
        "--no-owner",           # Don't include ownership commands
        "--no-privileges",      # Don't include privilege commands
        "-v",                   # Verbose output
    ]

    try:
        # Set password via environment variable
        env = {"PGPASSWORD": DB_PASSWORD}

        if compress:
            # Dump and compress on the fly
            print(f"   Compression: gzip")

            # Run pg_dump
            pg_dump_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )

            # Compress output
            with gzip.open(output_path, 'wb') as f_out:
                # Read from pg_dump and write compressed
                for line in pg_dump_process.stdout:
                    f_out.write(line)

            # Wait for completion
            return_code = pg_dump_process.wait()

            if return_code != 0:
                stderr = pg_dump_process.stderr.read().decode()
                print(f"❌ Error: {stderr}")
                return False

        else:
            # Regular dump to file
            cmd.extend(["-f", str(output_path)])

            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                print(f"❌ Error: {result.stderr}")
                return False

        # Check file size
        file_size = output_path.stat().st_size
        size_mb = file_size / (1024 * 1024)

        print(f"✅ Backup created successfully!")
        print(f"   Size: {size_mb:.2f} MB")
        print(f"   Path: {output_path}")

        return True

    except Exception as e:
        print(f"❌ Error creating backup: {e}")
        return False


def restore_backup(backup_path: Path) -> bool:
    """
    Restore database from a backup file.

    Args:
        backup_path: Path to backup file (.sql or .sql.gz)

    Returns:
        True if successful, False otherwise
    """
    print(f"🔄 Restoring database from backup...")
    print(f"   Backup: {backup_path}")
    print(f"   Target DB: {DB_NAME}")

    if not backup_path.exists():
        print(f"❌ Error: Backup file not found: {backup_path}")
        return False

    # Find psql binary
    try:
        psql_path = find_postgres_binary("psql")
        print(f"   Using: {psql_path}")
    except FileNotFoundError as e:
        print(f"❌ {e}")
        return False

    # Confirm before restore
    print("\n⚠️  WARNING: This will REPLACE all data in the database!")
    response = input("Are you sure? Type 'yes' to continue: ")
    if response.lower() != 'yes':
        print("Restore cancelled.")
        return False

    try:
        env = {"PGPASSWORD": DB_PASSWORD}

        # Determine if file is compressed
        is_compressed = str(backup_path).endswith('.gz')

        if is_compressed:
            print("   Decompressing and restoring...")
            # Decompress and pipe to psql
            with gzip.open(backup_path, 'rb') as f_in:
                cmd = [
                    psql_path,
                    "-h", DB_HOST,
                    "-p", DB_PORT,
                    "-U", DB_USER,
                    "-d", "postgres",  # Connect to postgres DB first
                    "-v", "ON_ERROR_STOP=1"
                ]

                result = subprocess.run(
                    cmd,
                    input=f_in.read(),
                    env=env,
                    capture_output=True
                )
        else:
            print("   Restoring from SQL file...")
            cmd = [
                psql_path,
                "-h", DB_HOST,
                "-p", DB_PORT,
                "-U", DB_USER,
                "-d", "postgres",
                "-f", str(backup_path),
                "-v", "ON_ERROR_STOP=1"
            ]

            result = subprocess.run(
                cmd,
                env=env,
                capture_output=True,
                text=True
            )

        if result.returncode != 0:
            stderr = result.stderr.decode() if isinstance(result.stderr, bytes) else result.stderr
            print(f"❌ Error: {stderr}")
            return False

        print(f"✅ Database restored successfully!")
        return True

    except Exception as e:
        print(f"❌ Error restoring backup: {e}")
        return False


def list_backups(backup_dir: Path):
    """List all available backups in the backup directory"""
    if not backup_dir.exists():
        print(f"No backups found (directory doesn't exist: {backup_dir})")
        return

    backups = sorted(
        backup_dir.glob("realestate_backup_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    if not backups:
        print(f"No backups found in: {backup_dir}")
        return

    print(f"\n📁 Available backups ({len(backups)}):\n")
    print(f"{'Filename':<40} {'Size':>10} {'Date':>20}")
    print("-" * 72)

    for backup in backups:
        size_mb = backup.stat().st_size / (1024 * 1024)
        mtime = datetime.fromtimestamp(backup.stat().st_mtime)
        date_str = mtime.strftime("%Y-%m-%d %H:%M:%S")
        print(f"{backup.name:<40} {size_mb:>9.2f}M {date_str:>20}")


def cleanup_old_backups(backup_dir: Path, keep_count: int = 7):
    """
    Remove old backups, keeping only the most recent N backups.

    Args:
        backup_dir: Directory containing backups
        keep_count: Number of recent backups to keep
    """
    if not backup_dir.exists():
        return

    backups = sorted(
        backup_dir.glob("realestate_backup_*"),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )

    if len(backups) <= keep_count:
        print(f"ℹ️  No cleanup needed ({len(backups)} backups, keeping {keep_count})")
        return

    to_delete = backups[keep_count:]
    print(f"🗑️  Cleaning up {len(to_delete)} old backup(s)...")

    for backup in to_delete:
        try:
            backup.unlink()
            print(f"   Deleted: {backup.name}")
        except Exception as e:
            print(f"   ⚠️  Failed to delete {backup.name}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="PostgreSQL backup and restore utility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create backup with automatic filename
  python -m backend.core.db_backup

  # Create compressed backup
  python -m backend.core.db_backup --compress

  # Create backup with custom path
  python -m backend.core.db_backup --output /tmp/my_backup.sql

  # Restore from backup
  python -m backend.core.db_backup --restore /path/to/backup.sql

  # List all backups
  python -m backend.core.db_backup --list

  # Cleanup old backups (keep last 5)
  python -m backend.core.db_backup --cleanup --keep 5
        """
    )

    parser.add_argument(
        "--output",
        type=Path,
        help="Output path for backup file (default: auto-generated in backups/)"
    )
    parser.add_argument(
        "--compress",
        action="store_true",
        help="Compress backup with gzip"
    )
    parser.add_argument(
        "--restore",
        type=Path,
        help="Restore database from backup file"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available backups"
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove old backups"
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=7,
        help="Number of backups to keep when cleaning up (default: 7)"
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=DEFAULT_BACKUP_DIR,
        help=f"Backup directory (default: {DEFAULT_BACKUP_DIR})"
    )

    args = parser.parse_args()

    # Ensure backup directory exists
    args.backup_dir.mkdir(parents=True, exist_ok=True)

    # List backups
    if args.list:
        list_backups(args.backup_dir)
        return

    # Cleanup old backups
    if args.cleanup:
        cleanup_old_backups(args.backup_dir, args.keep)
        return

    # Restore from backup
    if args.restore:
        success = restore_backup(args.restore)
        sys.exit(0 if success else 1)

    # Create backup
    output_path = args.output or args.backup_dir / create_backup_filename(args.compress)
    success = run_pg_dump(output_path, args.compress)

    if success:
        # Optionally cleanup old backups
        cleanup_old_backups(args.backup_dir, args.keep)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
