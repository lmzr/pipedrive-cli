"""Command-line interface for pipedrive-cli."""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import click
from frictionless import validate
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from . import __version__
from .api import PipedriveClient
from .backup import create_backup, describe_schemas
from .base import (
    add_schema_field,
    generate_local_field_key,
    get_entity_fields,
    load_package,
    load_records,
    remove_field_from_records,
    remove_schema_field,
    save_package,
    save_records,
    update_entity_fields,
)
from .config import ENTITIES
from .field import (
    TRANSFORMS,
    CopyStats,
    collect_unique_values,
    get_enum_options,
    prompt_add_options,
    transform_value,
)
from .matching import (
    AmbiguousMatchError,
    NoMatchError,
    find_field_by_key,
    match_entities,
    match_entity,
    match_field,
)
from .restore import restore_backup

console = Console()


def get_api_token() -> str:
    """Get API token from environment or prompt."""
    token = os.environ.get("PIPEDRIVE_API_TOKEN")
    if not token:
        raise click.ClickException(
            "PIPEDRIVE_API_TOKEN environment variable not set.\n"
            "Set it with: export PIPEDRIVE_API_TOKEN=your_token"
        )
    return token


def get_unique_output_dir(base: Path) -> Path:
    """Return a unique output directory, adding suffix if needed."""
    if not base.exists():
        return base

    suffix = 2
    while True:
        candidate = Path(f"{base}-{suffix}")
        if not candidate.exists():
            return candidate
        suffix += 1


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__)
def main() -> None:
    """Pipedrive CLI - Backup and export tool for Pipedrive CRM."""
    pass


@main.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory for backup (default: backup-YYYY-MM-DD)",
)
@click.option(
    "--entities",
    "-e",
    multiple=True,
    help="Specific entities to export (supports prefix matching, default: all)",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be exported without calling the API",
)
def backup(output: Path | None, entities: tuple[str, ...], dry_run: bool) -> None:
    """Create a full backup of Pipedrive data as a datapackage."""
    if output is None:
        base = Path(f"backup-{datetime.now().strftime('%Y-%m-%d')}")
        output = get_unique_output_dir(base)

    # Resolve entity prefixes to full names
    try:
        if entities:
            matched = match_entities(list(entities))
            entity_list = [e.name for e in matched]
        else:
            entity_list = list(ENTITIES.keys())
    except NoMatchError as e:
        raise click.ClickException(str(e))
    except AmbiguousMatchError as e:
        raise click.ClickException(str(e))

    if dry_run:
        console.print("[yellow]DRY RUN - no API calls will be made[/yellow]")
        console.print()
        console.print(f"[bold]Would create backup in:[/bold] {output}")
        console.print()

        table = Table(title="Entities to Export")
        table.add_column("Entity", style="cyan")
        table.add_column("Endpoint", style="dim")
        table.add_column("Has Schema", style="yellow")

        for name in entity_list:
            config = ENTITIES[name]
            has_schema = "Yes" if config.fields_endpoint else "No"
            table.add_row(name, config.endpoint, has_schema)

        console.print(table)
        console.print()
        console.print(f"[dim]Total entities:[/dim] {len(entity_list)}")
        return

    token = get_api_token()

    console.print(f"[bold]Creating backup in:[/bold] {output}")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Starting backup...", total=None)

        def update_progress(message: str | int) -> None:
            if isinstance(message, str):
                progress.update(task, description=message)

        package, counts = asyncio.run(
            create_backup(token, output, entity_list, progress_callback=update_progress)
        )

        progress.update(task, description="Backup complete!")

    console.print()
    console.print("[green]Backup created successfully![/green]")
    console.print(f"[dim]Location:[/dim] {output.absolute()}")
    console.print(f"[dim]Resources:[/dim] {len(package.resources)}")

    # Show summary table
    table = Table(title="Exported Entities")
    table.add_column("Entity", style="cyan")
    table.add_column("Count", style="green", justify="right")
    table.add_column("File", style="dim")

    total = 0
    for resource in package.resources:
        count = counts.get(resource.name, 0)
        total += count
        table.add_row(resource.name, str(count), resource.path)

    table.add_section()
    table.add_row("[bold]Total[/bold]", f"[bold]{total}[/bold]", "")

    console.print(table)


@main.command()
@click.option("--json", "-j", "output_json", is_flag=True, help="Output as JSON")
def describe(output_json: bool) -> None:
    """Show field schemas from Pipedrive API."""
    token = get_api_token()

    if not output_json:
        console.print("[bold]Fetching schemas from Pipedrive...[/bold]")

    schemas = asyncio.run(describe_schemas(token))

    if output_json:
        print(json.dumps(schemas, indent=2))
        return

    for entity_name, fields in schemas.items():
        if not fields:
            continue

        table = Table(title=f"{entity_name.title()} Fields")
        table.add_column("Key", style="cyan")
        table.add_column("Name", style="white")
        table.add_column("Type", style="yellow")
        table.add_column("Custom", style="dim")

        for field in fields:
            is_custom = "Yes" if field.get("is_subfield") or field.get("edit_flag") else ""
            table.add_row(
                field.get("key", ""),
                field.get("name", ""),
                field.get("field_type", ""),
                is_custom,
            )

        console.print(table)
        console.print()


@main.command("validate")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
def validate_backup(path: Path) -> None:
    """Validate an existing backup datapackage."""
    if path.is_dir():
        package_path = path / "datapackage.json"
    else:
        package_path = path

    if not package_path.exists():
        raise click.ClickException(f"datapackage.json not found in {path}")

    console.print(f"[bold]Validating:[/bold] {package_path}")

    # Validate directly from absolute path - frictionless resolves relative resources
    report = validate(str(package_path.absolute()))

    if report.valid:
        console.print("[green]Datapackage is valid![/green]")
    else:
        console.print("[red]Validation errors found:[/red]")
        for error in report.flatten(["type", "message"]):
            console.print(f"  - {error}")


@main.command()
def entities() -> None:
    """List available Pipedrive entities."""
    table = Table(title="Available Entities")
    table.add_column("Name", style="cyan")
    table.add_column("Endpoint", style="dim")
    table.add_column("Has Schema", style="yellow")

    for name, config in ENTITIES.items():
        has_schema = "Yes" if config.fields_endpoint else "No"
        table.add_row(name, config.endpoint, has_schema)

    console.print(table)


@main.command("store")
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be stored without making changes",
)
@click.option(
    "--entities",
    "-e",
    multiple=True,
    help="Specific entities to store (supports prefix matching, default: all)",
)
@click.option(
    "--log",
    "-l",
    type=click.Path(path_type=Path),
    default=None,
    help="Write detailed log to file (JSON lines format)",
)
@click.option(
    "--delete-extra-fields",
    is_flag=True,
    help="Delete custom fields not in backup (with confirmation)",
)
@click.option(
    "--delete-extra-records",
    is_flag=True,
    help="Delete records not in backup (with confirmation)",
)
@click.option(
    "--no-update-base",
    is_flag=True,
    help="Don't update local files with Pipedrive-assigned field keys",
)
def store(
    path: Path,
    dry_run: bool,
    entities: tuple[str, ...],
    log: Path | None,
    delete_extra_fields: bool,
    delete_extra_records: bool,
    no_update_base: bool,
) -> None:
    """Sync local data to Pipedrive.

    PATH is the backup directory containing datapackage.json.
    """
    token = get_api_token()

    # Resolve entity prefixes to full names
    try:
        if entities:
            matched = match_entities(list(entities))
            entity_list = [e.name for e in matched]
        else:
            entity_list = None
    except NoMatchError as e:
        raise click.ClickException(str(e))
    except AmbiguousMatchError as e:
        raise click.ClickException(str(e))

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")

    console.print(f"[bold]Storing from:[/bold] {path}")
    log_file = open(log, "w", encoding="utf-8") if log else None

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Starting store...", total=None)

            def update_progress(message: str) -> None:
                progress.update(task, description=message)

            report = asyncio.run(
                restore_backup(
                    token,
                    path,
                    entities=entity_list,
                    dry_run=dry_run,
                    delete_extra_fields=delete_extra_fields,
                    delete_extra_records=delete_extra_records,
                    update_base=not no_update_base,
                    log_file=log_file,
                    progress_callback=update_progress,
                )
            )

            progress.update(task, description="Store complete!")

    finally:
        if log_file:
            log_file.close()

    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
    else:
        console.print("[green]Store completed![/green]")

    if log:
        console.print(f"[dim]Log written to:[/dim] {log}")

    # Show field sync summary if any fields were created/deleted
    if report.field_stats:
        total_fields_created = sum(s.created for s in report.field_stats.values())
        total_fields_deleted = sum(s.deleted for s in report.field_stats.values())

        if total_fields_created or total_fields_deleted:
            field_table = Table(title="Field Sync Summary")
            field_table.add_column("Entity", style="cyan")
            field_table.add_column("Created", style="green", justify="right")
            field_table.add_column("Deleted", style="red", justify="right")

            for entity_name, stats in report.field_stats.items():
                if stats.created or stats.deleted:
                    field_table.add_row(
                        entity_name,
                        str(stats.created),
                        str(stats.deleted),
                    )

            field_table.add_section()
            field_table.add_row(
                "[bold]Total[/bold]",
                f"[bold]{total_fields_created}[/bold]",
                f"[bold]{total_fields_deleted}[/bold]",
            )

            console.print(field_table)
            console.print()

    # Show record summary table
    table = Table(title="Record Store Summary")
    table.add_column("Entity", style="cyan")
    table.add_column("Updated", style="blue", justify="right")
    table.add_column("Created", style="green", justify="right")
    table.add_column("Failed", style="red", justify="right")

    total_updated = 0
    total_created = 0
    total_failed = 0

    for entity_name, stats in report.record_stats.items():
        table.add_row(
            entity_name,
            str(stats.updated),
            str(stats.created),
            str(stats.failed),
        )
        total_updated += stats.updated
        total_created += stats.created
        total_failed += stats.failed

    table.add_section()
    table.add_row(
        "[bold]Total[/bold]",
        f"[bold]{total_updated}[/bold]",
        f"[bold]{total_created}[/bold]",
        f"[bold]{total_failed}[/bold]",
    )

    console.print(table)


# Keep 'restore' as hidden alias for backwards compatibility
main.add_command(store, name="restore")


# Field management commands


@main.group()
def field() -> None:
    """Manage Pipedrive custom fields."""
    pass


def is_custom_field(field_def: dict[str, Any]) -> bool:
    """Check if a field is a custom field (editable by user)."""
    return bool(field_def.get("edit_flag"))


@field.command("list")
@click.option(
    "--entity",
    "-e",
    required=True,
    help="Entity type (supports prefix matching: per, org, deal...)",
)
@click.option(
    "--base",
    "-b",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Read fields from local datapackage instead of API",
)
@click.option(
    "--custom-only",
    is_flag=True,
    help="Show only custom fields (edit_flag=True)",
)
def list_fields_cmd(
    entity: str,
    base: Path | None,
    custom_only: bool,
) -> None:
    """List fields for an entity.

    Shows field key, display name, and type for each field.

    Examples:

        # List all fields from API
        pipedrive-cli field list -e persons

        # List only custom fields
        pipedrive-cli field list -e persons --custom-only

        # List fields from backup data
        pipedrive-cli field list -e persons --base backup-2026-01-05/
    """
    # Resolve entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    if not matched_entity.fields_endpoint:
        raise click.ClickException(
            f"Entity '{matched_entity.name}' does not support custom fields"
        )

    # Get fields from base or API
    if base:
        try:
            package = load_package(base)
            fields = get_entity_fields(package, matched_entity.name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))

        if not fields:
            raise click.ClickException(
                f"No field definitions found for '{matched_entity.name}' in {base}"
            )
        source = f"from {base}"
    else:
        token = get_api_token()

        async def fetch():
            async with PipedriveClient(token) as client:
                return await client.fetch_fields(matched_entity)

        fields = asyncio.run(fetch())
        source = "from API"

    # Filter custom fields if requested
    if custom_only:
        fields = [f for f in fields if is_custom_field(f)]

    # Display table
    table = Table(title=f"{matched_entity.name.title()} Fields ({source})")
    table.add_column("Key", style="cyan")
    table.add_column("Name", style="white")
    table.add_column("Type", style="yellow")
    if not custom_only:
        table.add_column("Custom", style="dim")

    for field_def in fields:
        row = [
            field_def.get("key", ""),
            field_def.get("name", ""),
            field_def.get("field_type", ""),
        ]
        if not custom_only:
            row.append("Yes" if is_custom_field(field_def) else "")
        table.add_row(*row)

    console.print(table)
    console.print(f"[dim]Total: {len(fields)} fields[/dim]")


@field.command("delete")
@click.option(
    "--entity",
    "-e",
    required=True,
    help="Entity type (supports prefix matching: per, org, deal...)",
)
@click.option(
    "--field",
    "-f",
    required=True,
    help="Field key to delete (supports prefix matching)",
)
@click.option(
    "--base",
    "-b",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Delete field from local datapackage instead of API",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be deleted without making changes",
)
@click.option(
    "--force",
    is_flag=True,
    help="Skip confirmation prompt",
)
def delete_field_cmd(
    entity: str,
    field: str,
    base: Path | None,
    dry_run: bool,
    force: bool,
) -> None:
    """Delete a custom field.

    Only custom fields (edit_flag=True) can be deleted.
    System fields cannot be deleted.

    Examples:

        # Delete a field from API
        pipedrive-cli field delete -e persons -f abc123_status

        # Delete from local datapackage
        pipedrive-cli field delete -e persons -f abc123_status --base backup/

        # Dry run to see what would happen
        pipedrive-cli field delete -e persons -f abc123_status -n
    """
    # Resolve entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    if not matched_entity.fields_endpoint:
        raise click.ClickException(
            f"Entity '{matched_entity.name}' does not support custom fields"
        )

    # Get fields and match
    if base:
        try:
            package = load_package(base)
            fields = get_entity_fields(package, matched_entity.name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))

        if not fields:
            raise click.ClickException(
                f"No field definitions found for '{matched_entity.name}' in {base}"
            )
    else:
        token = get_api_token()

        async def fetch():
            async with PipedriveClient(token) as client:
                return await client.fetch_fields(matched_entity)

        fields = asyncio.run(fetch())

    # Match field with prefix matching
    try:
        matched_field_def = match_field(fields, field, confirm=True)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    field_key = matched_field_def["key"]
    field_name = matched_field_def.get("name", field_key)
    field_id = matched_field_def.get("id")

    # Check if custom field
    if not is_custom_field(matched_field_def):
        raise click.ClickException(
            f"Field '{field_key}' is a system field and cannot be deleted"
        )

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")

    console.print("[bold]Deleting field:[/bold]")
    console.print(f"  Entity: {matched_entity.name}")
    console.print(f"  Field key: {field_key}")
    console.print(f"  Field name: {field_name}")
    if base:
        console.print(f"  Source: {base}")
    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        return

    # Confirm deletion
    if not force:
        response = click.prompt(
            f"Delete field '{field_name}' ({field_key})? [y/N]",
            default="n",
            show_default=False,
        ).lower().strip()

        if response not in ("y", "yes"):
            console.print("[dim]Deletion cancelled.[/dim]")
            return

    # Delete field
    if base:
        # Remove from pipedrive_fields
        updated_fields = [f for f in fields if f.get("key") != field_key]
        update_entity_fields(package, matched_entity.name, updated_fields)

        # Remove from schema.fields
        remove_schema_field(package, matched_entity.name, field_key)

        # Remove column from CSV
        records = load_records(base, matched_entity.name)
        if records:
            records = remove_field_from_records(records, field_key)
            save_records(base, matched_entity.name, records)

        save_package(package, base)
        console.print(f"[green]Field '{field_key}' removed from {base}[/green]")
    else:
        # Delete via API
        async def delete():
            async with PipedriveClient(token) as client:
                await client.delete_field(matched_entity, field_id)

        asyncio.run(delete())
        console.print(f"[green]Field '{field_key}' deleted from Pipedrive[/green]")


async def _copy_field_values(
    token: str,
    entity_name: str,
    source_key: str,
    target_key: str,
    transform_type: str | None,
    format_str: str | None,
    separator: str | None,
    skip_null: bool,
    dry_run: bool,
    log_file: Any | None,
    progress_callback: Any | None,
) -> CopyStats:
    """Copy field values from source to target field."""
    stats = CopyStats()

    entity = match_entity(entity_name)

    async with PipedriveClient(token) as client:
        # Fetch field definitions
        if progress_callback:
            progress_callback("Fetching field definitions...")

        fields = await client.fetch_fields(entity)

        # Find source field
        source_field = find_field_by_key(fields, source_key)
        if not source_field:
            raise click.ClickException(f"Source field '{source_key}' not found")

        # Find target field
        target_field = find_field_by_key(fields, target_key)
        if not target_field:
            raise click.ClickException(f"Target field '{target_key}' not found")

        # Fetch all records first (needed for progress % and enum/set analysis)
        if progress_callback:
            progress_callback("Fetching all records...")

        all_records = []
        async for record in client.fetch_all(entity):
            all_records.append(record)

        total_records = len(all_records)

        # Handle enum/set auto-configuration
        if transform_type in ("enum", "set") and not dry_run:
            # Collect unique values from source
            unique_values = collect_unique_values(all_records, source_key)
            existing_options = get_enum_options(target_field)
            new_options = unique_values - existing_options

            if new_options:
                target_name = target_field.get("name", target_key)
                should_add = prompt_add_options(target_name, new_options, console)
                if should_add is None:
                    raise click.Abort()
                if should_add:
                    await client.add_field_options(entity, target_field["id"], list(new_options))
                    # Refresh target field
                    target_field = await client.get_field(entity, target_field["id"])

        # Process records
        if progress_callback:
            progress_callback(f"Processing 0/{total_records} (0%)...")

        record_count = 0
        for record in all_records:
            record_count += 1
            stats.total += 1
            record_id = record.get("id")

            source_value = record.get(source_key)

            # Skip null values if requested
            if source_value is None and skip_null:
                stats.skipped += 1
                if log_file:
                    log_file.write(json.dumps({
                        "record_id": record_id,
                        "action": "skipped",
                        "reason": "null_value",
                    }) + "\n")
                continue

            # Transform value
            result = transform_value(source_value, transform_type, format_str, separator)

            if not result.success:
                stats.failed += 1
                if log_file:
                    log_file.write(json.dumps({
                        "record_id": record_id,
                        "action": "failed",
                        "error": result.error,
                        "source_value": str(source_value),
                    }) + "\n")
                continue

            if dry_run:
                stats.copied += 1
                if log_file:
                    log_file.write(json.dumps({
                        "record_id": record_id,
                        "action": "would_copy",
                        "source_value": str(source_value),
                        "target_value": str(result.value),
                    }) + "\n")
            else:
                try:
                    await client.update(entity, record_id, {target_key: result.value})
                    stats.copied += 1
                    if log_file:
                        log_file.write(json.dumps({
                            "record_id": record_id,
                            "action": "copied",
                            "source_value": str(source_value),
                            "target_value": str(result.value),
                        }) + "\n")
                except Exception as e:
                    stats.failed += 1
                    if log_file:
                        log_file.write(json.dumps({
                            "record_id": record_id,
                            "action": "failed",
                            "error": str(e),
                        }) + "\n")

            if progress_callback and record_count % 10 == 0:
                pct = record_count * 100 // total_records
                progress_callback(f"Processing {record_count}/{total_records} ({pct}%)...")

    return stats


def _copy_field_local(
    base: Path,
    entity: Any,
    source_field: str,
    target_field: str,
    transform: str | None,
    format_str: str | None,
    separator: str | None,
    skip_null: bool,
    dry_run: bool,
    delete_source: bool,
    log: Path | None,
    exchange: bool,
) -> None:
    """Copy field values locally in a datapackage."""
    from .config import EntityConfig

    entity_config: EntityConfig = entity

    # Load datapackage and fields
    try:
        package = load_package(base)
        fields = get_entity_fields(package, entity_config.name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if not fields:
        raise click.ClickException(
            f"No field definitions found for '{entity_config.name}' in {base}"
        )

    # Match source field
    try:
        matched_source = match_field(fields, source_field, confirm=True)
        source_key = matched_source["key"]
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    # Find or determine target field
    target = find_field_by_key(fields, target_field)
    is_new_field = False
    if target:
        target_key = target["key"]
        target_name = target.get("name", target_key)
    else:
        # New field - generate a local key, use target_field as display name
        target_key = generate_local_field_key()  # e.g., "_new_7f3a2b"
        target_name = target_field  # Display name
        is_new_field = True

        # Create field definition for the new field
        new_field_def: dict[str, Any] = {
            "key": target_key,
            "name": target_name,
            "field_type": transform or "varchar",
            "edit_flag": True,  # Mark as custom field
        }
        fields.append(new_field_def)

        # Save field definition to datapackage (both pipedrive_fields and schema.fields)
        if not dry_run:
            update_entity_fields(package, entity_config.name, fields)
            add_schema_field(package, entity_config.name, target_key, "string")
            save_package(package, base)

    # Load records
    records = load_records(base, entity_config.name)
    if not records:
        console.print(f"[yellow]No records found for '{entity_config.name}' in {base}[/yellow]")
        return

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")

    console.print("[bold]Copying field values (local):[/bold]")
    console.print(f"  Entity: {entity_config.name}")
    console.print(f"  From: {source_key} ({matched_source.get('name', '')})")
    if is_new_field:
        console.print(f"  To: [dim]{target_key}[/dim] ({target_name}) [yellow]new field[/yellow]")
    else:
        console.print(f"  To: {target_key} ({target_name})")
    console.print(f"  Base: {base}")
    if transform:
        console.print(f"  Transform: {transform}")
    console.print()

    # Process records
    log_file = open(log, "w", encoding="utf-8") if log else None
    stats = CopyStats()

    try:
        for record in records:
            stats.total += 1
            record_id = record.get("id", stats.total)
            source_value = record.get(source_key)

            # Skip null values if requested
            if (source_value is None or source_value == "") and skip_null:
                stats.skipped += 1
                if log_file:
                    log_file.write(json.dumps({
                        "record_id": record_id,
                        "action": "skipped",
                        "reason": "null_value",
                    }) + "\n")
                continue

            # Transform value
            result = transform_value(source_value, transform, format_str, separator)

            if not result.success:
                stats.failed += 1
                if log_file:
                    log_file.write(json.dumps({
                        "record_id": record_id,
                        "action": "failed",
                        "error": result.error,
                        "source_value": str(source_value),
                    }) + "\n")
                continue

            # Apply the copy
            if not dry_run:
                record[target_key] = result.value

            stats.copied += 1
            if log_file:
                action = "would_copy" if dry_run else "copied"
                log_file.write(json.dumps({
                    "record_id": record_id,
                    "action": action,
                    "source_value": str(source_value),
                    "target_value": str(result.value),
                }) + "\n")

        # Save records
        if not dry_run:
            save_records(base, entity_config.name, records)

    finally:
        if log_file:
            log_file.close()

    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
    else:
        console.print("[green]Field copy completed![/green]")

    if log:
        console.print(f"[dim]Log written to:[/dim] {log}")

    # Show summary
    table = Table(title="Copy Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green", justify="right")

    table.add_row("Total records", str(stats.total))
    table.add_row("Copied", str(stats.copied))
    table.add_row("Skipped", str(stats.skipped))
    table.add_row("Failed", str(stats.failed))

    console.print(table)

    # Handle --exchange (swap display names)
    if exchange:
        source_name = matched_source.get("name", source_key)
        # Reload fields to get latest (in case new field was created)
        fields = get_entity_fields(package, entity_config.name)
        # Find target field def (may be newly created)
        target_def = find_field_by_key(fields, target_key)
        target_display_name = target_def.get("name", target_key) if target_def else target_name

        if dry_run:
            console.print()
            console.print(
                f"[yellow]Would exchange names:[/yellow] {source_name} ↔ {target_display_name}"
            )
        else:
            # Swap names in fields list
            for f in fields:
                if f.get("key") == source_key:
                    f["name"] = target_display_name
                elif f.get("key") == target_key:
                    f["name"] = source_name

            update_entity_fields(package, entity_config.name, fields)
            save_package(package, base)
            console.print()
            console.print(
                f"[green]Exchanged names:[/green] {source_name} ↔ {target_display_name}"
            )

    # Handle delete-source (remove field from records and field definitions)
    if delete_source and not dry_run and stats.failed == 0:
        console.print()
        response = click.prompt(
            f"Delete source field '{source_key}' from local data? [y/N]",
            default="n",
            show_default=False,
        ).lower().strip()

        if response in ("y", "yes"):
            # Remove field values from records
            for record in records:
                if source_key in record:
                    del record[source_key]
            save_records(base, entity_config.name, records)

            # Remove field definition
            updated_fields = [f for f in fields if f.get("key") != source_key]
            update_entity_fields(package, entity_config.name, updated_fields)
            save_package(package, base)

            console.print(f"[green]Source field '{source_key}' removed from {base}[/green]")
        else:
            console.print("[dim]Source field not deleted.[/dim]")


# Pipedrive field types for --create-type option
PIPEDRIVE_FIELD_TYPES = [
    "varchar", "varchar_auto", "text", "int", "double", "monetary",
    "date", "daterange", "time", "timerange", "phone", "enum", "set",
    "user", "org", "people", "address", "visible_to",
]


@field.command("copy")
@click.option(
    "--entity",
    "-e",
    required=True,
    help="Entity type (supports prefix matching: per, org, deal...)",
)
@click.option(
    "--from",
    "-f",
    "source_field",
    required=True,
    help="Source field key (supports prefix matching)",
)
@click.option(
    "--to",
    "-t",
    "target_field",
    required=True,
    help="Target field key (existing) or name (with --create-type)",
)
@click.option(
    "--create-type",
    "-c",
    "create_type",
    type=click.Choice(PIPEDRIVE_FIELD_TYPES),
    default=None,
    help="Create target field with this Pipedrive type (--to becomes field name)",
)
@click.option(
    "--base",
    "-b",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Copy field values in local datapackage instead of API",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be copied without making changes",
)
@click.option(
    "--log",
    "-l",
    type=click.Path(path_type=Path),
    default=None,
    help="Write detailed log to file (JSON lines format)",
)
@click.option(
    "--delete-source",
    is_flag=True,
    help="Delete source field after successful copy (with confirmation)",
)
@click.option(
    "--skip-null",
    is_flag=True,
    help="Skip records where source field is null",
)
@click.option(
    "--transform",
    type=click.Choice(list(TRANSFORMS.keys())),
    default=None,
    help="Transform values to target type (int, double, varchar, text, date, enum, set)",
)
@click.option(
    "--format",
    "format_str",
    default=None,
    help="Format string for transformation (dates: %%d/%%m/%%Y, numbers: .2f)",
)
@click.option(
    "--separator",
    default=None,
    help="Separator for set↔varchar conversion (default: ',' input, ', ' output)",
)
@click.option(
    "--exchange",
    "-x",
    is_flag=True,
    help="Exchange display names between source and target fields after copy",
)
def copy_field_cmd(
    entity: str,
    source_field: str,
    target_field: str,
    create_type: str | None,
    base: Path | None,
    dry_run: bool,
    log: Path | None,
    delete_source: bool,
    skip_null: bool,
    transform: str | None,
    format_str: str | None,
    separator: str | None,
    exchange: bool,
) -> None:
    """Copy values from one field to another.

    Useful for migrating data when changing field types in Pipedrive.

    When --create-type or --transform is provided, the other is inferred
    automatically (both use Pipedrive type names).

    Examples:

        # Copy to existing field
        pipedrive-cli field copy -e persons -f old_key -t new_key

        # Create new enum field (--transform inferred from --create-type)
        pipedrive-cli field copy -e persons -f status_text -t "Status" -c enum

        # Create new date field (--create-type inferred from --transform)
        pipedrive-cli field copy -e deals -f date_str -t "Date" \\
            --transform date --format "%d/%m/%Y"

        # Copy in local datapackage
        pipedrive-cli field copy -e persons -f old_key -t new_key --base backup/

        # Dry run to see what would be copied
        pipedrive-cli field copy -e persons -f old -t new_key -n
    """
    # Warn if --delete-source without --transform (should use rename instead)
    if delete_source and not transform and not create_type:
        console.print(
            "[yellow]Note: --delete-source without --transform is equivalent to rename.[/yellow]"
        )
        console.print("[yellow]Consider using 'field rename' instead.[/yellow]")
        console.print()

    # Infer transform from create_type if not specified (they use same type names)
    if create_type and not transform and create_type in TRANSFORMS:
        transform = create_type

    # Resolve entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    if not matched_entity.fields_endpoint:
        raise click.ClickException(f"Entity '{matched_entity.name}' does not support custom fields")

    # Handle --base (local mode)
    if base:
        _copy_field_local(
            base=base,
            entity=matched_entity,
            source_field=source_field,
            target_field=target_field,
            transform=transform,
            format_str=format_str,
            separator=separator,
            skip_null=skip_null,
            dry_run=dry_run,
            delete_source=delete_source,
            log=log,
            exchange=exchange,
        )
        return

    # API mode
    token = get_api_token()

    # Resolve source field prefix
    async def get_fields_and_match():
        async with PipedriveClient(token) as client:
            fields = await client.fetch_fields(matched_entity)
            return fields, match_field(fields, source_field, confirm=True)

    try:
        fields, matched_source = asyncio.run(get_fields_and_match())
        source_key = matched_source["key"]
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    # Handle target field: existing key or create new
    target = find_field_by_key(fields, target_field)
    target_key: str
    target_name: str
    created_field = False

    # Infer create_type from transform if target doesn't exist
    effective_create_type = create_type
    if not target and not create_type and transform:
        effective_create_type = transform

    target_id: int | None = None  # Track for exchange

    if target:
        # Target field exists
        target_key = target["key"]
        target_name = target.get("name", target_key)
        target_id = target.get("id")
        if create_type:
            console.print(
                f"[yellow]Warning: --create-type ignored, field '{target_key}' exists[/yellow]"
            )
    elif effective_create_type:
        # Create new field
        if dry_run:
            console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
            console.print(
                f"[dim]Would create field '{target_field}' of type '{effective_create_type}'[/dim]"
            )
            target_key = f"<new:{target_field}>"
            target_name = target_field
        else:
            console.print(
                f"Creating field '{target_field}' of type '{effective_create_type}'..."
            )

            async def create_target_field():
                async with PipedriveClient(token) as client:
                    return await client.create_field(
                        matched_entity, target_field, effective_create_type
                    )

            new_field = asyncio.run(create_target_field())
            target_key = new_field["key"]
            target_name = new_field.get("name", target_field)
            target_id = new_field.get("id")
            created_field = True
            console.print(f"[green]Created field '{target_key}'[/green]")
    else:
        raise click.ClickException(
            f"Target field '{target_field}' not found. "
            f"Use --create-type/-c or --transform to create it."
        )

    if dry_run and not created_field:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")

    console.print("[bold]Copying field values:[/bold]")
    console.print(f"  Entity: {matched_entity.name}")
    console.print(f"  From: {source_key} ({matched_source.get('name', '')})")
    console.print(f"  To: {target_key} ({target_name})")
    if transform:
        console.print(f"  Transform: {transform}")
    console.print()

    log_file = open(log, "w", encoding="utf-8") if log else None

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Starting copy...", total=None)

            def update_progress(message: str) -> None:
                progress.update(task, description=message)

            stats = asyncio.run(
                _copy_field_values(
                    token=token,
                    entity_name=matched_entity.name,
                    source_key=source_key,
                    target_key=target_key,
                    transform_type=transform,
                    format_str=format_str,
                    separator=separator,
                    skip_null=skip_null,
                    dry_run=dry_run,
                    log_file=log_file,
                    progress_callback=update_progress,
                )
            )

            progress.update(task, description="Copy complete!")

    finally:
        if log_file:
            log_file.close()

    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
    else:
        console.print("[green]Field copy completed![/green]")

    if log:
        console.print(f"[dim]Log written to:[/dim] {log}")

    # Show summary
    table = Table(title="Copy Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="green", justify="right")

    table.add_row("Total records", str(stats.total))
    table.add_row("Copied", str(stats.copied))
    table.add_row("Skipped", str(stats.skipped))
    table.add_row("Failed", str(stats.failed))

    console.print(table)

    # Handle --exchange (swap display names via API)
    if exchange:
        source_name = matched_source.get("name", source_key)
        source_id = matched_source.get("id")

        if dry_run:
            console.print()
            console.print(f"[yellow]Would exchange names:[/yellow] {source_name} ↔ {target_name}")
        elif source_id and target_id:
            async def exchange_names():
                async with PipedriveClient(token) as client:
                    await client.update_field(matched_entity, source_id, name=target_name)
                    await client.update_field(matched_entity, target_id, name=source_name)

            asyncio.run(exchange_names())
            console.print()
            console.print(f"[green]Exchanged names:[/green] {source_name} ↔ {target_name}")
        else:
            console.print()
            console.print("[red]Could not exchange names: missing field IDs[/red]")

    # Handle delete-source
    if delete_source and not dry_run and stats.failed == 0:
        console.print()
        response = click.prompt(
            f"Delete source field '{source_key}'? [y/N/q]",
            default="n",
            show_default=False,
        ).lower().strip()

        if response == "q":
            raise click.Abort()
        if response in ("y", "yes"):
            async def delete_field():
                async with PipedriveClient(token) as client:
                    await client.delete_field(matched_entity, matched_source["id"])

            asyncio.run(delete_field())
            console.print(f"[green]Source field '{source_key}' deleted.[/green]")
        else:
            console.print("[dim]Source field not deleted.[/dim]")


@field.command("rename")
@click.option(
    "--entity",
    "-e",
    required=True,
    help="Entity type (supports prefix matching: per, org, deal...)",
)
@click.option(
    "--field",
    "-f",
    required=True,
    help="Field key to rename (supports prefix matching)",
)
@click.option(
    "--name",
    "-o",
    "new_name",
    required=True,
    help="New display name for the field",
)
@click.option(
    "--base",
    "-b",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Rename field in local datapackage instead of API",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be renamed without making changes",
)
def rename_field_cmd(
    entity: str,
    field: str,
    new_name: str,
    base: Path | None,
    dry_run: bool,
) -> None:
    """Rename a field's display name.

    Changes the 'name' property of a field (the label shown in Pipedrive UI).
    The field key remains unchanged.

    Examples:

        # Rename a field's display name
        pipedrive-cli field rename -e persons -f abc123_status -o "New Status"

        # With prefix matching
        pipedrive-cli field rename -e per -f abc123 -o "Better Name"

        # Rename in local datapackage
        pipedrive-cli field rename -e persons -f abc123 -o "New Name" --base backup/

        # Dry run to see what would happen
        pipedrive-cli field rename -e persons -f abc123_status -o "New Name" -n
    """
    # Resolve entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    if not matched_entity.fields_endpoint:
        raise click.ClickException(
            f"Entity '{matched_entity.name}' does not support custom fields"
        )

    # Get fields and match
    if base:
        try:
            package = load_package(base)
            fields = get_entity_fields(package, matched_entity.name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))

        if not fields:
            raise click.ClickException(
                f"No field definitions found for '{matched_entity.name}' in {base}"
            )
    else:
        token = get_api_token()

        async def fetch():
            async with PipedriveClient(token) as client:
                return await client.fetch_fields(matched_entity)

        fields = asyncio.run(fetch())

    # Match field with prefix matching
    try:
        matched_field_def = match_field(fields, field, confirm=True)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    field_key = matched_field_def["key"]
    field_id = matched_field_def.get("id")
    old_name = matched_field_def.get("name", "")

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")

    console.print("[bold]Renaming field:[/bold]")
    console.print(f"  Entity: {matched_entity.name}")
    console.print(f"  Field key: {field_key}")
    console.print(f"  Current name: {old_name}")
    console.print(f"  New name: {new_name}")
    if base:
        console.print(f"  Source: {base}")
    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        return

    # Update field name
    if base:
        # Update in datapackage
        for f in fields:
            if f.get("key") == field_key:
                f["name"] = new_name
                break
        update_entity_fields(package, matched_entity.name, fields)
        save_package(package, base)
        console.print(f"[green]Field '{field_key}' renamed to '{new_name}' in {base}[/green]")
    else:
        # Update via API
        async def update_name():
            async with PipedriveClient(token) as client:
                return await client.update_field(matched_entity, field_id, name=new_name)

        asyncio.run(update_name())
        console.print(f"[green]Field '{field_key}' renamed to '{new_name}'[/green]")


if __name__ == "__main__":
    main()
