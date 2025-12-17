#!/usr/bin/env python3
"""
S3 JSONL Upload Script with Best Practices

Uploads JSONL files from a local directory to S3 with proper partitioning,
validation, and metadata tracking.

Usage:
    python scripts/upload_to_s3.py --source-dir ./data --bucket cda-raw-dev --client wise
    python scripts/upload_to_s3.py --source-dir ./data --dry-run
    python scripts/upload_to_s3.py --source-dir ./data --force --run-date 2025-12-17
"""

import gzip
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys

import boto3
import click
from botocore.exceptions import ClientError, NoCredentialsError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich import print as rprint

console = Console()


class FileMetadata:
    """Metadata for a file to upload"""
    def __init__(self, local_path: Path, source: str, table: str):
        self.local_path = local_path
        self.source = source
        self.table = table
        self.size = local_path.stat().st_size
        
    def __repr__(self):
        return f"FileMetadata(source={self.source}, table={self.table}, file={self.local_path.name})"


def parse_filename(filename: str) -> Tuple[str, str]:
    """
    Parse filename to extract source and table name.
    
    Examples:
        stripe_customers.jsonl -> (stripe, customers)
        sf_accounts.jsonl -> (salesforce, accounts)
        zendesk_tickets.jsonl -> (zendesk, tickets)
    
    Returns:
        Tuple of (source_name, table_name)
    """
    # Remove .jsonl extension
    name_without_ext = filename.replace('.jsonl', '')
    
    # Split on first underscore
    parts = name_without_ext.split('_', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid filename format: {filename}. Expected format: source_table.jsonl")
    
    source_prefix, table_name = parts
    
    # Map source prefixes to full source names
    source_mapping = {
        'stripe': 'stripe',
        'sf': 'salesforce',
        'zendesk': 'zendesk',
        'harvest': 'harvest',
        'jira': 'jira',
        'mixpanel': 'mixpanel',
        'intacct': 'intacct',
    }
    
    source = source_mapping.get(source_prefix, source_prefix)
    
    return source, table_name


def validate_jsonl(file_path: Path) -> Tuple[bool, Optional[str], int]:
    """
    Validate that a file is valid JSONL format.
    
    Returns:
        Tuple of (is_valid, error_message, line_count)
    """
    try:
        line_count = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    json.loads(line)
                    line_count += 1
                except json.JSONDecodeError as e:
                    return False, f"Invalid JSON at line {line_num}: {e}", line_count
        
        if line_count == 0:
            return False, "File is empty", 0
            
        return True, None, line_count
    except Exception as e:
        return False, f"Error reading file: {e}", 0


def scan_directory(source_dir: Path) -> List[FileMetadata]:
    """
    Scan directory for JSONL files and extract metadata.
    
    Returns:
        List of FileMetadata objects
    """
    jsonl_files = list(source_dir.glob("*.jsonl"))
    
    if not jsonl_files:
        console.print(f"[yellow]No JSONL files found in {source_dir}[/yellow]")
        return []
    
    file_metadata = []
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Scanning files...", total=len(jsonl_files))
        
        for file_path in jsonl_files:
            try:
                source, table = parse_filename(file_path.name)
                
                # Validate JSONL format
                is_valid, error_msg, line_count = validate_jsonl(file_path)
                if not is_valid:
                    console.print(f"[red]✗[/red] {file_path.name}: {error_msg}")
                    progress.update(task, advance=1)
                    continue
                
                file_metadata.append(FileMetadata(file_path, source, table))
                console.print(f"[green]✓[/green] {file_path.name}: {line_count} records")
                
            except ValueError as e:
                console.print(f"[red]✗[/red] {file_path.name}: {e}")
            
            progress.update(task, advance=1)
    
    return file_metadata


def generate_s3_path(
    client: str,
    source: str,
    table: str,
    run_date: str,
    filename: str,
    compress: bool = True
) -> str:
    """
    Generate S3 key following the pattern:
    clients/{client}/{source}/{table}/run_date=YYYY-MM-DD/{filename}
    
    If compress is True, adds .gz extension.
    """
    compressed_suffix = ".gz" if compress else ""
    return f"clients/{client}/{source}/{table}/run_date={run_date}/{filename}{compressed_suffix}"


def generate_success_marker_path(
    client: str,
    source: str,
    table: str,
    run_date: str
) -> str:
    """
    Generate S3 key for _SUCCESS marker file.
    """
    return f"clients/{client}/{source}/{table}/run_date={run_date}/_SUCCESS"


def check_file_exists(s3_client, bucket: str, key: str) -> bool:
    """Check if a file exists in S3"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise


def compress_file(local_path: Path) -> Path:
    """Compress a file using GZIP and return path to compressed file."""
    compressed_path = Path(tempfile.gettempdir()) / f"{local_path.name}.gz"
    
    with open(local_path, 'rb') as f_in:
        with gzip.open(compressed_path, 'wb', compresslevel=6) as f_out:
            f_out.write(f_in.read())
    
    return compressed_path


def upload_file(
    s3_client,
    local_path: Path,
    bucket: str,
    s3_key: str,
    compress: bool = True,
    dry_run: bool = False
) -> bool:
    """Upload a single file to S3, optionally compressing it first."""
    if dry_run:
        compression_suffix = ".gz" if compress else ""
        console.print(f"[dim]Would upload: {local_path.name}{compression_suffix} -> s3://{bucket}/{s3_key}[/dim]")
        return True
    
    try:
        # Compress file if requested
        if compress:
            compressed_path = compress_file(local_path)
            upload_path = compressed_path
            content_encoding = 'gzip'
        else:
            upload_path = local_path
            content_encoding = None
        
        extra_args = {
            'ServerSideEncryption': 'AES256',
            'Metadata': {
                'uploaded-at': datetime.utcnow().isoformat(),
                'original-filename': local_path.name,
            }
        }
        
        if content_encoding:
            extra_args['ContentEncoding'] = content_encoding
        
        s3_client.upload_file(
            str(upload_path),
            bucket,
            s3_key,
            ExtraArgs=extra_args
        )
        
        # Clean up compressed file
        if compress:
            compressed_path.unlink()
        
        return True
    except Exception as e:
        console.print(f"[red]Error uploading {local_path.name}: {e}[/red]")
        # Clean up compressed file on error
        if compress and 'compressed_path' in locals():
            try:
                compressed_path.unlink()
            except:
                pass
        return False


def upload_success_marker(
    s3_client,
    bucket: str,
    s3_key: str,
    file_count: int,
    dry_run: bool = False
) -> bool:
    """Upload _SUCCESS marker file"""
    if dry_run:
        console.print(f"[dim]Would create marker: s3://{bucket}/{s3_key}[/dim]")
        return True
    
    try:
        marker_content = json.dumps({
            'uploaded_at': datetime.utcnow().isoformat(),
            'file_count': file_count,
            'status': 'complete'
        })
        
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=marker_content.encode('utf-8'),
            ServerSideEncryption='AES256',
            ContentType='application/json'
        )
        return True
    except Exception as e:
        console.print(f"[red]Error creating _SUCCESS marker: {e}[/red]")
        return False


@click.command()
@click.option(
    '--source-dir',
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    default=Path('./data'),
    help='Source directory containing JSONL files'
)
@click.option(
    '--bucket',
    type=str,
    default='cda-raw-dev',
    help='S3 bucket name'
)
@click.option(
    '--client',
    type=str,
    default='wise',
    help='Client name for S3 prefix'
)
@click.option(
    '--run-date',
    type=str,
    default=datetime.utcnow().strftime('%Y-%m-%d'),
    help='Run date for partitioning (YYYY-MM-DD format)'
)
@click.option(
    '--dry-run',
    is_flag=True,
    help='Preview changes without uploading'
)
@click.option(
    '--force',
    is_flag=True,
    help='Overwrite existing files'
)
@click.option(
    '--region',
    type=str,
    default='eu-west-2',
    help='AWS region'
)
@click.option(
    '--aws-profile',
    type=str,
    default=None,
    help='AWS profile name to use for credentials'
)
@click.option(
    '--no-compress',
    is_flag=True,
    help='Disable GZIP compression (files uploaded as .jsonl instead of .jsonl.gz)'
)
def main(
    source_dir: Path,
    bucket: str,
    client: str,
    run_date: str,
    dry_run: bool,
    force: bool,
    region: str,
    aws_profile: Optional[str],
    no_compress: bool
):
    """
    Upload JSONL files to S3 with best practices.
    
    This script uploads JSONL files from a local directory to S3 with:
    - GZIP compression (.jsonl.gz format)
    - Hive-style partitioning (run_date=YYYY-MM-DD)
    - Table-level organization
    - JSONL validation
    - _SUCCESS marker files
    - Progress tracking
    """
    console.print("\n[bold cyan]S3 JSONL Upload Tool[/bold cyan]\n")
    
    # Determine compression setting
    compress = not no_compress
    
    # Validate run_date format
    try:
        datetime.strptime(run_date, '%Y-%m-%d')
    except ValueError:
        console.print("[red]Error: run-date must be in YYYY-MM-DD format[/red]")
        sys.exit(1)
    
    # Display configuration
    config_table = Table(title="Configuration")
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="green")
    config_table.add_row("Source Directory", str(source_dir))
    config_table.add_row("S3 Bucket", bucket)
    config_table.add_row("Client", client)
    config_table.add_row("Run Date", run_date)
    config_table.add_row("Region", region)
    config_table.add_row("AWS Profile", aws_profile if aws_profile else "default")
    config_table.add_row("Compression", "GZIP (.gz)" if compress else "None")
    config_table.add_row("Dry Run", "Yes" if dry_run else "No")
    config_table.add_row("Force Overwrite", "Yes" if force else "No")
    console.print(config_table)
    console.print()
    
    # Scan directory
    console.print("[bold]Step 1: Scanning and validating files[/bold]")
    file_metadata = scan_directory(source_dir)
    
    if not file_metadata:
        console.print("\n[yellow]No valid files to upload[/yellow]")
        sys.exit(0)
    
    console.print(f"\n[green]Found {len(file_metadata)} valid files[/green]\n")
    
    # Initialize S3 client
    if not dry_run:
        try:
            # Create session with profile if specified
            if aws_profile:
                session = boto3.Session(profile_name=aws_profile, region_name=region)
                s3_client = session.client('s3')
            else:
                s3_client = boto3.client('s3', region_name=region)
            
            # Test credentials and bucket access
            s3_client.head_bucket(Bucket=bucket)
        except NoCredentialsError:
            console.print("[red]Error: AWS credentials not found. Configure AWS CLI first.[/red]")
            sys.exit(1)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                console.print(f"[red]Error: Bucket '{bucket}' not found[/red]")
            elif e.response['Error']['Code'] == '403':
                console.print(f"[red]Error: Access denied to bucket '{bucket}'[/red]")
            else:
                console.print(f"[red]Error: {e}[/red]")
            sys.exit(1)
    else:
        s3_client = None
    
    # Group files by source/table for _SUCCESS markers
    source_table_groups: Dict[Tuple[str, str], List[FileMetadata]] = {}
    for fm in file_metadata:
        key = (fm.source, fm.table)
        if key not in source_table_groups:
            source_table_groups[key] = []
        source_table_groups[key].append(fm)
    
    # Display upload plan
    console.print("[bold]Step 2: Upload plan[/bold]")
    plan_table = Table()
    plan_table.add_column("File", style="cyan")
    plan_table.add_column("Source", style="yellow")
    plan_table.add_column("Table", style="magenta")
    plan_table.add_column("Size", style="green")
    plan_table.add_column("S3 Path", style="dim")
    
    for fm in file_metadata:
        s3_key = generate_s3_path(client, fm.source, fm.table, run_date, fm.local_path.name, compress=compress)
        size_mb = fm.size / (1024 * 1024)
        display_size = f"{size_mb:.2f} MB" + (" → ~{:.2f} MB (gz)".format(size_mb * 0.3) if compress else "")
        plan_table.add_row(
            fm.local_path.name,
            fm.source,
            fm.table,
            display_size,
            s3_key
        )
    
    console.print(plan_table)
    console.print()
    
    if dry_run:
        console.print("[yellow]Dry run mode - no files were uploaded[/yellow]")
        return
    
    # Upload files
    console.print("[bold]Step 3: Uploading files[/bold]")
    
    uploaded_count = 0
    skipped_count = 0
    failed_count = 0
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console
    ) as progress:
        task = progress.add_task("Uploading...", total=len(file_metadata))
        
        for fm in file_metadata:
            s3_key = generate_s3_path(client, fm.source, fm.table, run_date, fm.local_path.name, compress=compress)
            
            # Check if file exists
            if not force and check_file_exists(s3_client, bucket, s3_key):
                console.print(f"[yellow]⊙[/yellow] {fm.local_path.name}: Already exists (use --force to overwrite)")
                skipped_count += 1
                progress.update(task, advance=1)
                continue
            
            # Upload file (with compression if enabled)
            if upload_file(s3_client, fm.local_path, bucket, s3_key, compress=compress, dry_run=False):
                console.print(f"[green]✓[/green] {fm.local_path.name}: Uploaded")
                uploaded_count += 1
            else:
                failed_count += 1
            
            progress.update(task, advance=1)
    
    # Upload _SUCCESS markers
    console.print("\n[bold]Step 4: Creating _SUCCESS markers[/bold]")
    
    for (source, table), files in source_table_groups.items():
        marker_path = generate_success_marker_path(client, source, table, run_date)
        if upload_success_marker(s3_client, bucket, marker_path, len(files), dry_run=False):
            console.print(f"[green]✓[/green] Created marker for {source}/{table}")
    
    # Summary
    console.print("\n[bold cyan]Upload Summary[/bold cyan]")
    summary_table = Table()
    summary_table.add_column("Status", style="bold")
    summary_table.add_column("Count", style="cyan")
    summary_table.add_row("✓ Uploaded", str(uploaded_count))
    summary_table.add_row("⊙ Skipped", str(skipped_count))
    summary_table.add_row("✗ Failed", str(failed_count))
    summary_table.add_row("Total", str(len(file_metadata)))
    console.print(summary_table)
    
    if failed_count > 0:
        console.print("\n[red]Some files failed to upload. Check the logs above.[/red]")
        sys.exit(1)
    elif uploaded_count == 0 and skipped_count > 0:
        console.print("\n[yellow]All files already exist. Use --force to overwrite.[/yellow]")
    else:
        console.print("\n[green]✓ Upload completed successfully![/green]")


if __name__ == '__main__':
    main()
