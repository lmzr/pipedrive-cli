"""Command-line interface for pipedrive-cli."""

import asyncio
import copy
import json
import os
import shutil
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
from .backup import (
    PIPEDRIVE_TO_FRICTIONLESS_TYPES,
    SUPPORTED_FIELD_TYPES,
    create_backup,
    describe_schemas,
)
from .base import (
    add_schema_field,
    create_field_definition,
    diff_field_metadata,
    generate_local_field_key,
    get_csv_columns,
    get_entity_fields,
    load_package,
    load_records,
    merge_field_metadata,
    remove_field_from_records,
    remove_schema_field,
    save_package,
    save_records,
    update_entity_fields,
)
from .config import ENTITIES
from .converter import (
    ConvertResult,
    detect_output_format,
    load_xlsx,
    write_csv,
    write_json,
)
from .exceptions import PipedriveError
from .field import (
    TRANSFORMS,
    CopyStats,
    build_option_lookup,
    collect_unique_values,
    get_enum_options,
    get_option_usage,
    prompt_add_options,
    sync_options_with_data,
    transform_value,
)
from .importer import (
    import_records,
    load_input_file,
    validate_input_fields,
)
from .matching import (
    AmbiguousMatchError,
    NoMatchError,
    find_field_by_key,
    match_entities,
    match_entity,
    match_field,
    prompt_field_choice,
)
from .restore import restore_backup
from .search import (
    FilterError,
    extract_filter_keys,
    filter_record,
    format_csv,
    format_json,
    format_resolved_expression,
    format_table,
    preprocess_record_for_filter,
    resolve_field_prefixes,
    resolve_filter_expression,
    select_fields,
    validate_expression,
)
from .transform import (
    apply_update_local,
    evaluate_assignment,
    format_resolved_assignment,
    parse_assignment,
    resolve_assignment,
)

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
@click.argument("fields", nargs=-1, required=True)
def delete_field_cmd(
    entity: str,
    base: Path | None,
    dry_run: bool,
    force: bool,
    fields: tuple[str, ...],
) -> None:
    """Delete custom field(s).

    Only custom fields (edit_flag=True) can be deleted.
    System fields cannot be deleted.

    Examples:

        # Delete a single field
        pipedrive-cli field delete -e persons abc123_status

        # Delete multiple fields
        pipedrive-cli field delete -e persons field1 field2 field3 --force

        # Delete from local datapackage
        pipedrive-cli field delete -e per field1 field2 --base backup/

        # Dry run to see what would happen
        pipedrive-cli field delete -e persons abc123_status -n
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

    # Get fields
    token: str | None = None
    if base:
        try:
            package = load_package(base)
            all_fields = get_entity_fields(package, matched_entity.name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))

        if not all_fields:
            raise click.ClickException(
                f"No field definitions found for '{matched_entity.name}' in {base}"
            )
    else:
        token = get_api_token()

        async def fetch():
            async with PipedriveClient(token) as client:
                return await client.fetch_fields(matched_entity)

        all_fields = asyncio.run(fetch())

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
        console.print()

    # Track results for summary
    results: list[tuple[str, str, str]] = []  # (key, name, status)

    for field_input in fields:
        # Match field with prefix matching
        try:
            matched_field_def = match_field(all_fields, field_input, confirm=True)
        except NoMatchError:
            results.append((field_input, field_input, "Not found"))
            console.print(f"[red]Field '{field_input}' not found[/red]")
            continue
        except AmbiguousMatchError as e:
            results.append((field_input, field_input, "Ambiguous"))
            console.print(f"[red]{e}[/red]")
            continue
        except click.Abort:
            results.append((field_input, field_input, "Cancelled"))
            continue

        field_key = matched_field_def["key"]
        field_name = matched_field_def.get("name", field_key)
        field_id = matched_field_def.get("id")

        # Check if custom field
        if not is_custom_field(matched_field_def):
            results.append((field_key, field_name, "System field"))
            console.print(f"[red]Cannot delete system field '{field_key}'[/red]")
            continue

        if dry_run:
            results.append((field_key, field_name, "Would delete"))
            console.print(f"[dim]Would delete:[/dim] {field_key} ({field_name})")
            continue

        # Confirm deletion (unless --force)
        if not force:
            response = click.prompt(
                f"Delete field '{field_name}' ({field_key})? [y/N]",
                default="n",
                show_default=False,
            ).lower().strip()

            if response not in ("y", "yes"):
                results.append((field_key, field_name, "Skipped"))
                console.print(f"[dim]Skipped: {field_key}[/dim]")
                continue

        # Delete field
        if base:
            # Remove from pipedrive_fields
            all_fields = [f for f in all_fields if f.get("key") != field_key]
            update_entity_fields(package, matched_entity.name, all_fields)

            # Remove from schema.fields
            remove_schema_field(package, matched_entity.name, field_key)

            # Remove column from CSV
            records = load_records(base, matched_entity.name)
            if records:
                records = remove_field_from_records(records, field_key)
                save_records(base, matched_entity.name, records)

            save_package(package, base)
            results.append((field_key, field_name, "Deleted"))
            console.print(f"[green]Deleted:[/green] {field_key} ({field_name})")
        else:
            # Delete via API
            async def delete_api():
                async with PipedriveClient(token) as client:
                    await client.delete_field(matched_entity, field_id)

            asyncio.run(delete_api())
            # Remove from local list to avoid re-matching
            all_fields = [f for f in all_fields if f.get("key") != field_key]
            results.append((field_key, field_name, "Deleted"))
            console.print(f"[green]Deleted:[/green] {field_key} ({field_name})")

    # Show summary table if multiple fields
    if len(fields) > 1:
        console.print()
        table = Table(title="Delete Summary")
        table.add_column("Field Key", style="cyan")
        table.add_column("Name", style="dim")
        table.add_column("Status", style="green")

        for key, name, status in results:
            style = "green" if status == "Deleted" else "yellow" if "Would" in status else "red"
            table.add_row(key, name, f"[{style}]{status}[/{style}]")

        console.print(table)

    if dry_run:
        console.print()
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")


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


@field.command("create")
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
    required=True,
    help="Target datapackage directory",
)
@click.argument("name")
@click.option(
    "--type",
    "-t",
    "field_type",
    required=True,
    type=click.Choice(SUPPORTED_FIELD_TYPES),
    help="Pipedrive field type",
)
@click.option(
    "--options",
    "-o",
    multiple=True,
    help="Options for enum/set fields (can be repeated)",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be created without making changes",
)
def create_field_cmd(
    entity: str,
    base: Path,
    name: str,
    field_type: str,
    options: tuple[str, ...],
    dry_run: bool,
) -> None:
    """Create a new custom field in a local datapackage.

    Creates a field definition with a local key (_new_*) that will be
    synchronized to Pipedrive when using the 'store' command.

    NAME is the display name for the new field.

    For enum or set fields, use --options to specify the available values.

    Examples:

        # Create a text field
        pipedrive-cli field create -e persons -b backup/ "Notes" -t text

        # Create an enum field with options
        pipedrive-cli field create -e persons -b backup/ "Category" -t enum \\
          -o "POWER GEEK" -o "POWER USER" -o "LEADER" -o "PROSPECT"

        # Create a multi-select field
        pipedrive-cli field create -e persons -b backup/ "Tags" -t set \\
          -o "VIP" -o "Partner" -o "Lead"

        # Create a reference field
        pipedrive-cli field create -e persons -b backup/ "Parent Company" -t org

        # Dry run to preview
        pipedrive-cli field create -e per -b backup/ "Test Field" -t varchar -n
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

    # Load datapackage
    try:
        package = load_package(base)
        fields = get_entity_fields(package, matched_entity.name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if not fields:
        raise click.ClickException(
            f"No field definitions found for '{matched_entity.name}' in {base}"
        )

    # Check if field name already exists
    existing_names = {f.get("name", "").lower() for f in fields}
    if name.lower() in existing_names:
        raise click.ClickException(
            f"A field with name '{name}' already exists for {matched_entity.name}"
        )

    # Validate options for enum/set
    if field_type in ("enum", "set") and not options:
        raise click.ClickException(
            f"Field type '{field_type}' requires at least one --options value"
        )

    if field_type not in ("enum", "set") and options:
        console.print(
            f"[yellow]Warning: --options ignored for field type '{field_type}'[/yellow]"
        )

    # Create field definition
    field_def = create_field_definition(name, field_type, list(options) if options else None)
    field_key = field_def["key"]

    # Get Frictionless type for schema
    frictionless_type = PIPEDRIVE_TO_FRICTIONLESS_TYPES.get(field_type, "string")

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
        console.print()

    console.print("[bold]Creating field:[/bold]")
    console.print(f"  Entity: {matched_entity.name}")
    console.print(f"  Name: {name}")
    console.print(f"  Key: {field_key}")
    console.print(f"  Type: {field_type} → {frictionless_type}")
    if options:
        console.print(f"  Options: {', '.join(options)}")
    console.print(f"  Target: {base}")
    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        return

    # Add field to pipedrive_fields
    fields.append(field_def)
    update_entity_fields(package, matched_entity.name, fields)

    # Add field to Frictionless schema
    add_schema_field(package, matched_entity.name, field_key, frictionless_type)

    # Save package
    save_package(package, base)

    # Add empty column to CSV records
    records = load_records(base, matched_entity.name)
    if records:
        for record in records:
            record[field_key] = ""
        save_records(base, matched_entity.name, records)

    console.print(f"[green]Field '{name}' created with key '{field_key}'[/green]")
    console.print()
    console.print("[dim]Use 'store' command to sync this field to Pipedrive.[/dim]")


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


# Field options subgroup
@field.group("options")
def field_options() -> None:
    """Manage enum/set field options."""
    pass


@field_options.command("list")
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
    help="Read from local datapackage instead of API",
)
@click.option(
    "--field",
    "-f",
    required=True,
    help="Field key (supports prefix matching)",
)
@click.option(
    "--show-usage",
    is_flag=True,
    help="Show usage count per option (requires --base)",
)
def options_list_cmd(
    entity: str,
    base: Path | None,
    field: str,
    show_usage: bool,
) -> None:
    """List options of an enum/set field.

    Shows all available options for a field, with optional usage statistics.

    Examples:

        # List options from API
        pipedrive-cli field options list -e persons -f category

        # List options from local backup with usage count
        pipedrive-cli field options list -e per -b backup/ -f status --show-usage
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

    # Get fields
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

    # Match field
    try:
        matched_field = match_field(fields, field, confirm=True)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    field_key = matched_field["key"]
    field_name = matched_field.get("name", field_key)
    field_type = matched_field.get("field_type", "")

    # Check field type
    if field_type not in ("enum", "set"):
        raise click.ClickException(
            f"Field '{field_name}' is type '{field_type}', not enum or set"
        )

    options = matched_field.get("options", [])

    # Get usage counts if requested and base provided
    usage: dict[str, int] = {}
    if show_usage:
        if not base:
            console.print("[yellow]Warning: --show-usage requires --base, ignoring[/yellow]")
        else:
            records = load_records(base, matched_entity.name)
            usage = get_option_usage(records, field_key, options)

    # Display table
    table = Table(title=f"Options for '{field_name}' ({source})")
    table.add_column("ID", style="dim", justify="right")
    table.add_column("Label", style="cyan")
    if usage:
        table.add_column("Usage", style="green", justify="right")

    for opt in options:
        row = [str(opt.get("id", "")), opt.get("label", "")]
        if usage:
            count = usage.get(opt.get("label", ""), 0)
            row.append(str(count))
        table.add_row(*row)

    console.print(table)
    console.print(f"[dim]Total: {len(options)} options[/dim]")


@field_options.command("add")
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
    required=True,
    help="Target datapackage directory",
)
@click.option(
    "--field",
    "-f",
    required=True,
    help="Field key (supports prefix matching)",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be added without making changes",
)
@click.argument("values", nargs=-1, required=True)
def options_add_cmd(
    entity: str,
    base: Path,
    field: str,
    dry_run: bool,
    values: tuple[str, ...],
) -> None:
    """Add options to an enum/set field.

    VALUES are the option labels to add.

    Examples:

        # Add a single option
        pipedrive-cli field options add -e per -b backup/ -f category "New Type"

        # Add multiple options
        pipedrive-cli field options add -e deals -b backup/ -f priority \\
          "Critical" "High" "Medium" "Low"

        # Dry run
        pipedrive-cli field options add -e per -b backup/ -f status "Test" -n
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

    # Load datapackage
    try:
        package = load_package(base)
        fields = get_entity_fields(package, matched_entity.name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if not fields:
        raise click.ClickException(
            f"No field definitions found for '{matched_entity.name}' in {base}"
        )

    # Match field
    try:
        matched_field = match_field(fields, field, confirm=True)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    field_key = matched_field["key"]
    field_name = matched_field.get("name", field_key)
    field_type = matched_field.get("field_type", "")

    # Check field type
    if field_type not in ("enum", "set"):
        raise click.ClickException(
            f"Field '{field_name}' is type '{field_type}', not enum or set"
        )

    current_options = matched_field.get("options", [])
    current_labels = {opt.get("label", "") for opt in current_options}

    # Check for duplicates
    duplicates = [v for v in values if v in current_labels]
    if duplicates:
        raise click.ClickException(
            f"Options already exist: {', '.join(duplicates)}"
        )

    # Generate new options with IDs
    max_id = max((opt.get("id", 0) for opt in current_options), default=0)
    new_options = [
        {"id": max_id + i + 1, "label": label}
        for i, label in enumerate(values)
    ]

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
        console.print()

    console.print(f"[bold]Adding options to '{field_name}':[/bold]")
    for opt in new_options:
        console.print(f"  [{opt['id']}] {opt['label']}")
    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        return

    # Update field definition
    matched_field["options"] = current_options + new_options

    # Find and update in fields list
    for i, f in enumerate(fields):
        if f.get("key") == field_key:
            fields[i] = matched_field
            break

    update_entity_fields(package, matched_entity.name, fields)
    save_package(package, base)

    console.print(f"[green]Added {len(new_options)} options to '{field_name}'[/green]")


@field_options.command("remove")
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
    required=True,
    help="Target datapackage directory",
)
@click.option(
    "--field",
    "-f",
    required=True,
    help="Field key (supports prefix matching)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Remove even if options are in use",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be removed without making changes",
)
@click.argument("values", nargs=-1, required=True)
def options_remove_cmd(
    entity: str,
    base: Path,
    field: str,
    force: bool,
    dry_run: bool,
    values: tuple[str, ...],
) -> None:
    """Remove options from an enum/set field.

    VALUES are the option labels to remove.

    By default, refuses to remove options that are in use by records.
    Use --force to override this check.

    Examples:

        # Remove an option
        pipedrive-cli field options remove -e per -b backup/ -f category "Old Type"

        # Force remove even if in use
        pipedrive-cli field options remove -e per -b backup/ -f status "Deprecated" --force
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

    # Load datapackage
    try:
        package = load_package(base)
        fields = get_entity_fields(package, matched_entity.name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if not fields:
        raise click.ClickException(
            f"No field definitions found for '{matched_entity.name}' in {base}"
        )

    # Match field
    try:
        matched_field = match_field(fields, field, confirm=True)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    field_key = matched_field["key"]
    field_name = matched_field.get("name", field_key)
    field_type = matched_field.get("field_type", "")

    # Check field type
    if field_type not in ("enum", "set"):
        raise click.ClickException(
            f"Field '{field_name}' is type '{field_type}', not enum or set"
        )

    current_options = matched_field.get("options", [])
    current_labels = {opt.get("label", "") for opt in current_options}

    # Check values exist
    not_found = [v for v in values if v not in current_labels]
    if not_found:
        raise click.ClickException(
            f"Options not found: {', '.join(not_found)}"
        )

    # Check usage if not forced
    records = load_records(base, matched_entity.name)
    usage = get_option_usage(records, field_key, current_options)

    in_use = [(v, usage.get(v, 0)) for v in values if usage.get(v, 0) > 0]
    if in_use and not force:
        console.print("[red]Cannot remove options in use:[/red]")
        for label, count in in_use:
            console.print(f"  - '{label}': {count} records")
        console.print()
        raise click.ClickException("Use --force to remove anyway")

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
        console.print()

    console.print(f"[bold]Removing options from '{field_name}':[/bold]")
    for v in values:
        count = usage.get(v, 0)
        if count > 0:
            console.print(f"  - {v} [yellow]({count} records affected)[/yellow]")
        else:
            console.print(f"  - {v}")
    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        return

    # Filter out removed options
    values_set = set(values)
    new_options = [opt for opt in current_options if opt.get("label") not in values_set]
    matched_field["options"] = new_options

    # Find and update in fields list
    for i, f in enumerate(fields):
        if f.get("key") == field_key:
            fields[i] = matched_field
            break

    update_entity_fields(package, matched_entity.name, fields)
    save_package(package, base)

    console.print(f"[green]Removed {len(values)} options from '{field_name}'[/green]")


@field_options.command("sync")
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
    required=True,
    help="Target datapackage directory",
)
@click.option(
    "--field",
    "-f",
    required=True,
    help="Field key (supports prefix matching)",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be synced without making changes",
)
def options_sync_cmd(
    entity: str,
    base: Path,
    field: str,
    dry_run: bool,
) -> None:
    """Sync field options with values found in data.

    Analyzes the values used in records and adds missing options.
    Also reports options that are defined but not used.

    Examples:

        # Sync options with data
        pipedrive-cli field options sync -e persons -b backup/ -f category

        # Dry run to see what would change
        pipedrive-cli field options sync -e per -b backup/ -f status -n
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

    # Load datapackage
    try:
        package = load_package(base)
        fields = get_entity_fields(package, matched_entity.name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if not fields:
        raise click.ClickException(
            f"No field definitions found for '{matched_entity.name}' in {base}"
        )

    # Match field
    try:
        matched_field = match_field(fields, field, confirm=True)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))
    except click.Abort:
        return

    field_key = matched_field["key"]
    field_name = matched_field.get("name", field_key)
    field_type = matched_field.get("field_type", "")

    # Check field type
    if field_type not in ("enum", "set"):
        raise click.ClickException(
            f"Field '{field_name}' is type '{field_type}', not enum or set"
        )

    current_options = matched_field.get("options", [])
    records = load_records(base, matched_entity.name)

    # Sync options with data
    updated_options, added_labels, unused_labels = sync_options_with_data(
        records, field_key, current_options
    )

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
        console.print()

    console.print(f"[bold]Syncing options for '{field_name}':[/bold]")
    console.print()

    # Show added options
    if added_labels:
        console.print("[green]Options to add (found in data):[/green]")
        for label in added_labels:
            console.print(f"  + {label}")
        console.print()
    else:
        console.print("[dim]No missing options to add[/dim]")
        console.print()

    # Show unused options
    if unused_labels:
        console.print("[yellow]Unused options (defined but not in data):[/yellow]")
        for label in unused_labels:
            console.print(f"  ? {label}")
        console.print()
        console.print("[dim]Note: Use 'field options remove' to delete unused options[/dim]")
        console.print()
    else:
        console.print("[dim]No unused options[/dim]")
        console.print()

    if not added_labels:
        console.print("[dim]Nothing to sync[/dim]")
        return

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        return

    # Update field definition
    matched_field["options"] = updated_options

    # Find and update in fields list
    for i, f in enumerate(fields):
        if f.get("key") == field_key:
            fields[i] = matched_field
            break

    update_entity_fields(package, matched_entity.name, fields)
    save_package(package, base)

    console.print(f"[green]Added {len(added_labels)} options to '{field_name}'[/green]")


# Record operations group
@main.group()
def record() -> None:
    """Record operations (search, import, update)."""
    pass


@record.command("search")
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
    help="Search in local datapackage instead of API",
)
@click.option(
    "--filter",
    "-f",
    "filter_expr",
    default=None,
    help="Filter expression (e.g., \"contains(name, 'John') and age > 30\")",
)
@click.option(
    "--include",
    "-i",
    default=None,
    help="Comma-separated field prefixes to include in output",
)
@click.option(
    "--exclude",
    "-x",
    default=None,
    help="Comma-separated field prefixes to exclude from output",
)
@click.option(
    "--format",
    "-o",
    "output_format",
    type=click.Choice(["table", "json", "csv"]),
    default="table",
    help="Output format (default: table)",
)
@click.option(
    "--limit",
    "-l",
    type=int,
    default=None,
    help="Maximum number of records to return",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show resolved filter expression only, without executing search",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Don't show resolved filter expression before results",
)
def record_search(
    entity: str,
    base: Path | None,
    filter_expr: str | None,
    include: str | None,
    exclude: str | None,
    output_format: str,
    limit: int | None,
    dry_run: bool,
    quiet: bool,
) -> None:
    """Search and filter records from Pipedrive data.

    Filter expressions use simpleeval syntax with custom string functions:

    \b
    Functions:
      contains(field, substr)    Case-insensitive substring match
      startswith(field, prefix)  Case-insensitive prefix match
      endswith(field, suffix)    Case-insensitive suffix match
      isnull(field)              Check if field is null or empty
      notnull(field)             Check if field is not null
      len(field)                 Get string length

    \b
    Operators: >, <, >=, <=, ==, !=, and, or, not

    \b
    Field Resolution:
      Field identifiers are resolved by key prefix, then name prefix.
      Use exact keys or prefixes: "abc123" resolves to "abc123_custom_field"

    Examples:

        # Search persons from API
        pipedrive-cli search -e persons

        # Search with filter (local)
        pipedrive-cli search -e per --base backup/ -f "contains(name, 'John')"

        # Verify filter resolution (dry-run)
        pipedrive-cli search -e per -f "contains(First, 'John')" -n

        # Output as JSON
        pipedrive-cli search -e deals -f "value > 10000" -o json -q

        # Include only specific fields
        pipedrive-cli search -e per -i "id,name,email" --limit 10
    """
    # Resolve entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    # Get token for API mode
    token = None
    if not base:
        token = get_api_token()

    # Load fields for resolution
    if base:
        try:
            package = load_package(base)
            fields = get_entity_fields(package, matched_entity.name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))

        if not fields:
            # Try to infer fields from CSV columns
            fields = []
    else:
        async def fetch_fields():
            async with PipedriveClient(token) as client:
                return await client.fetch_fields(matched_entity)

        if matched_entity.fields_endpoint:
            fields = asyncio.run(fetch_fields())
        else:
            fields = []

    # Resolve filter expression
    resolved_expr = None
    resolutions: dict[str, tuple[str, str]] = {}
    filter_keys: list[str] = []
    # Use interactive prompt for ambiguous fields unless quiet mode
    on_ambiguous = prompt_field_choice if not quiet else None
    if filter_expr:
        try:
            resolved_expr, resolutions = resolve_filter_expression(
                fields, filter_expr, on_ambiguous=on_ambiguous
            )
            filter_keys = extract_filter_keys(fields, resolved_expr)
        except AmbiguousMatchError as e:
            raise click.ClickException(f"Ambiguous field in filter: {e}")

        # Validate expression before use
        try:
            field_keys = {f["key"] for f in fields}
            validate_expression(resolved_expr, field_keys)
        except FilterError as e:
            raise click.ClickException(str(e))

        # Show resolved expression (unless quiet)
        if not quiet:
            name_line, key_line = format_resolved_expression(
                filter_expr, resolved_expr, resolutions
            )
            if key_line:
                # Resolution happened - show both name and key versions
                console.print(f"[dim]Filter w/ names: {name_line}[/dim]")
                console.print(f"[dim]Filter w/ keys:  {key_line}[/dim]")
            else:
                # No resolution needed
                console.print(f"[dim]Filter: {resolved_expr}[/dim]")

    # Dry-run mode: exit after showing resolved expression
    if dry_run:
        if not resolved_expr:
            console.print("[yellow]No filter expression provided for dry-run[/yellow]")
        else:
            console.print("[dim](dry-run: search not executed)[/dim]")
        return

    # Resolve include/exclude field prefixes
    include_keys = None
    exclude_keys = None

    if include:
        prefixes = [p.strip() for p in include.split(",")]
        try:
            include_keys = resolve_field_prefixes(fields, prefixes, fail_on_ambiguous=False)
        except AmbiguousMatchError as e:
            raise click.ClickException(str(e))
        # Always include 'id' for reference
        if "id" not in include_keys:
            include_keys.insert(0, "id")
        # Include filter fields so they appear in output
        for key in filter_keys:
            if key not in include_keys:
                include_keys.append(key)

    if exclude:
        prefixes = [p.strip() for p in exclude.split(",")]
        try:
            exclude_keys = resolve_field_prefixes(fields, prefixes, fail_on_ambiguous=False)
        except AmbiguousMatchError as e:
            raise click.ClickException(str(e))

    # Load records
    if base:
        records = load_records(base, matched_entity.name)
        if not records:
            console.print(
                f"[yellow]No records found for '{matched_entity.name}' in {base}[/yellow]"
            )
            return
    else:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(f"Fetching {matched_entity.name}...", total=None)

            async def fetch_records():
                records = []
                async with PipedriveClient(token) as client:
                    async for record in client.fetch_all(matched_entity):
                        records.append(record)
                        progress.update(task, description=f"Fetched {len(records)} records...")
                return records

            records = asyncio.run(fetch_records())

        if not records:
            console.print(f"[yellow]No records found for '{matched_entity.name}'[/yellow]")
            return

    # Apply filter
    if resolved_expr:
        try:
            # Build option lookup for enum/set field comparison
            option_lookup = build_option_lookup(fields) if fields else {}
            filtered = [
                r for r in records
                if filter_record(preprocess_record_for_filter(r, option_lookup), resolved_expr)
            ]
        except FilterError as e:
            raise click.ClickException(str(e))
    else:
        filtered = records

    # Apply limit
    if limit and limit > 0:
        filtered = filtered[:limit]

    # Apply field selection
    selected = [select_fields(r, include_keys, exclude_keys) for r in filtered]

    # Output
    if output_format == "json":
        print(format_json(selected))
    elif output_format == "csv":
        print(format_csv(selected))
    else:
        # Show all columns if user specified --include (they chose what to see)
        format_table(
            selected,
            fields,
            console,
            title=f"{matched_entity.name.title()} Search",
            show_all_columns=bool(include_keys),
            filter_keys=filter_keys,
        )


# Alias for backward compatibility: 'search' -> 'record search'
# We need to copy the command to avoid sharing hidden state
_search_alias = copy.copy(record_search)
_search_alias.hidden = True
main.add_command(_search_alias, name="search")


# Data operations group
@main.group()
def data() -> None:
    """Data operations (convert, validate)."""
    pass


@data.command("convert")
@click.argument("input_file", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    required=True,
    help="Output file path",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["csv", "json"]),
    default=None,
    help="Output format (auto-detect from extension if not specified)",
)
@click.option(
    "--sheet",
    "-s",
    default=None,
    help="Sheet name for XLSX files (default: first sheet)",
)
@click.option(
    "--header-row",
    "-r",
    type=int,
    default=1,
    help="Row number containing headers (default: 1)",
)
@click.option(
    "--preserve-links",
    is_flag=True,
    help="Replace cell values with hyperlink URLs when available",
)
def convert_cmd(
    input_file: Path,
    output: Path,
    output_format: str | None,
    sheet: str | None,
    header_row: int,
    preserve_links: bool,
) -> None:
    """Convert XLSX files to CSV or JSON format.

    Supports extraction of hyperlinks from XLSX cells using --preserve-links.

    INPUT_FILE is the source XLSX file to convert.

    Examples:

        # Convert XLSX to CSV
        pipedrive-cli data convert contacts.xlsx -o contacts.csv

        # Convert specific sheet to JSON
        pipedrive-cli data convert data.xlsx -o output.json -s "Sheet 2"

        # Extract hyperlinks (URLs) instead of display text
        pipedrive-cli data convert links.xlsx -o links.csv --preserve-links

        # Specify custom header row
        pipedrive-cli data convert report.xlsx -o report.csv -r 3
    """
    # Check input format
    input_suffix = input_file.suffix.lower()
    if input_suffix != ".xlsx":
        raise click.ClickException(
            f"Unsupported input format '{input_suffix}'. Only XLSX is supported."
        )

    # Determine output format
    if output_format:
        fmt = output_format
    else:
        fmt = detect_output_format(output)

    console.print(f"[bold]Converting:[/bold] {input_file}")
    console.print(f"[bold]Output:[/bold] {output} ({fmt.upper()})")
    if sheet:
        console.print(f"[bold]Sheet:[/bold] {sheet}")
    if header_row != 1:
        console.print(f"[bold]Header row:[/bold] {header_row}")
    if preserve_links:
        console.print("[bold]Preserve links:[/bold] Yes")
    console.print()

    # Load XLSX
    result: ConvertResult = load_xlsx(
        input_file,
        sheet=sheet,
        header_row=header_row,
        preserve_links=preserve_links,
    )

    # Write output
    if fmt == "csv":
        write_csv(result.records, result.fieldnames, output)
    else:
        write_json(result.records, output)

    # Show stats
    console.print("[green]Conversion complete![/green]")
    console.print(f"  Rows: {result.stats.total_rows}")
    console.print(f"  Columns: {result.stats.total_columns}")
    if result.stats.hyperlinks_found > 0:
        console.print(f"  Hyperlinks found: {result.stats.hyperlinks_found}")
        if preserve_links:
            console.print(f"  Hyperlinks preserved: {result.stats.hyperlinks_preserved}")
    console.print(f"  Output: {output}")


# Note: 'import' is a Python reserved word, so we use 'import_' as function name
@record.command("import")
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
    required=True,
    help="Target datapackage directory",
)
@click.option(
    "--input",
    "-i",
    "input_file",
    type=click.Path(exists=True, path_type=Path),
    required=True,
    help="Input file (CSV, JSON, or XLSX)",
)
@click.option(
    "--format",
    "file_format",
    type=click.Choice(["csv", "json", "xlsx"]),
    default=None,
    help="Input format (auto-detect from extension if not specified)",
)
@click.option(
    "--key",
    "-k",
    default=None,
    help="Field(s) for deduplication (comma-separated)",
)
@click.option(
    "--on-duplicate",
    type=click.Choice(["update", "skip", "error"]),
    default="update",
    help="Action on duplicate key (default: update)",
)
@click.option(
    "--auto-id",
    is_flag=True,
    help="Generate IDs for new records without id field",
)
@click.option(
    "--sheet",
    "-s",
    default=None,
    help="Sheet name for XLSX files (default: first sheet)",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be imported without making changes",
)
@click.option(
    "--log",
    "-l",
    type=click.Path(path_type=Path),
    default=None,
    help="Write detailed log to file (JSON lines format)",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Suppress verbose output",
)
def import_cmd(
    entity: str,
    base: Path,
    input_file: Path,
    file_format: str | None,
    key: str | None,
    on_duplicate: str,
    auto_id: bool,
    sheet: str | None,
    dry_run: bool,
    log: Path | None,
    quiet: bool,
) -> None:
    """Import records from CSV/JSON/XLSX into local datapackage.

    Validates input fields against the datapackage schema. All fields must
    exist in the schema (use 'field create' to add missing fields first).

    System fields (id, add_time, etc.) are automatically skipped.

    Examples:

        # Import CSV with auto-ID generation
        pipedrive-cli record import -e persons -b backup/ -i contacts.csv --auto-id

        # Import with deduplication by email
        pipedrive-cli record import -e persons -b backup/ -i new_data.csv \\
          -k email --on-duplicate update

        # Import XLSX from specific sheet
        pipedrive-cli record import -e deals -b backup/ -i sales.xlsx -s "Q4 Data"

        # Skip duplicates instead of updating
        pipedrive-cli record import -e orgs -b backup/ -i orgs.json \\
          -k name --on-duplicate skip

        # Dry run with logging
        pipedrive-cli record import -e per -b backup/ -i data.csv -n -l import.jsonl
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

    # Load datapackage
    try:
        package = load_package(base)
        fields = get_entity_fields(package, matched_entity.name)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if not fields:
        raise click.ClickException(
            f"No field definitions found for '{matched_entity.name}' in {base}"
        )

    # Load input file
    try:
        input_records, input_fieldnames = load_input_file(
            input_file, file_format=file_format, sheet=sheet
        )
    except Exception as e:
        raise click.ClickException(f"Error loading input file: {e}")

    if not input_records:
        raise click.ClickException("Input file contains no records")

    # Validate fields
    valid_fields, readonly_skipped, unknown_fields = validate_input_fields(
        input_fieldnames, fields
    )

    # Error on unknown fields
    if unknown_fields:
        raise click.ClickException(
            f"Unknown fields in input: {', '.join(unknown_fields)}. "
            "Use 'field create' to add them first."
        )

    # Parse key fields
    key_fields = None
    if key:
        key_fields = [k.strip() for k in key.split(",")]
        # Validate key fields exist
        for kf in key_fields:
            if kf not in valid_fields and kf not in readonly_skipped:
                raise click.ClickException(f"Key field '{kf}' not found in input")

    # Show summary
    if not quiet:
        console.print(f"[bold]Importing to:[/bold] {matched_entity.name} in {base}")
        console.print(f"[bold]Input:[/bold] {input_file}")
        console.print(f"[bold]Records:[/bold] {len(input_records)}")
        console.print(f"[bold]Fields:[/bold] {len(valid_fields)} valid")
        if readonly_skipped:
            console.print(f"[dim]  Skipping read-only: {', '.join(readonly_skipped)}[/dim]")
        if key_fields:
            console.print(f"[bold]Deduplication key:[/bold] {', '.join(key_fields)}")
            console.print(f"[bold]On duplicate:[/bold] {on_duplicate}")
        if auto_id:
            console.print("[bold]Auto-ID:[/bold] enabled")
        console.print()

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
        console.print()

    # Load existing records
    existing_records = load_records(base, matched_entity.name)

    # Open log file if specified
    log_file = None
    if log:
        log_file = open(log, "w", encoding="utf-8")

    try:
        # Import records
        stats, merged_records, results = import_records(
            input_records,
            existing_records,
            valid_fields,
            key_fields=key_fields,
            on_duplicate=on_duplicate,
            auto_id=auto_id,
            log_file=log_file if not dry_run else None,
            field_defs=fields,
            base_path=base,
        )

        # Store readonly_skipped in stats
        stats.readonly_skipped = readonly_skipped

        # Show results
        console.print("[bold]Import Results:[/bold]")
        console.print(f"  Total processed: {stats.total}")
        console.print(f"  Created: [green]{stats.created}[/green]")
        console.print(f"  Updated: [blue]{stats.updated}[/blue]")
        console.print(f"  Skipped: [yellow]{stats.skipped}[/yellow]")
        if stats.failed > 0:
            console.print(f"  Failed: [red]{stats.failed}[/red]")
            for error in stats.errors[:5]:
                console.print(f"    - {error}")
            if len(stats.errors) > 5:
                console.print(f"    ... and {len(stats.errors) - 5} more errors")
        console.print()

        if dry_run:
            console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
            if log:
                # Write dry-run log
                with open(log, "w", encoding="utf-8") as f:
                    for result in results:
                        log_entry = {
                            "row": result.row_number,
                            "action": result.action,
                            "id": result.record_id,
                        }
                        if result.error:
                            log_entry["error"] = result.error
                        f.write(json.dumps(log_entry, ensure_ascii=False) + "\n")
                console.print(f"[dim]Log written to: {log}[/dim]")
            return

        # Save merged records
        save_records(base, matched_entity.name, merged_records)

        console.print("[green]Import complete![/green]")
        if log:
            console.print(f"[dim]Log written to: {log}[/dim]")

    finally:
        if log_file:
            log_file.close()


@record.command("update")
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
    help="Update records in local datapackage instead of API",
)
@click.option(
    "--filter",
    "-f",
    "filter_expr",
    default=None,
    help="Filter expression to select records (e.g., \"contains(name, 'John')\")",
)
@click.option(
    "--set",
    "-s",
    "assignments",
    multiple=True,
    required=True,
    help="Field assignment 'field=expr' (can be repeated)",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Preview changes without applying them",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Don't show resolved expressions before results",
)
@click.option(
    "--log",
    "-l",
    type=click.Path(path_type=Path),
    default=None,
    help="Write detailed log to file (JSON lines format)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to update",
)
def record_update(
    entity: str,
    base: Path | None,
    filter_expr: str | None,
    assignments: tuple[str, ...],
    dry_run: bool,
    quiet: bool,
    log: Path | None,
    limit: int | None,
) -> None:
    """Update field values on records matching a filter.

    Apply transformation expressions to modify field values in bulk.
    Uses simpleeval syntax with extended functions for string/numeric operations.

    \b
    Assignment Format:
      --set "field=expression"   Field identifier and expression
      Field identifiers are resolved like search filters (key prefix, name prefix)
      Expression can reference any record fields and use transform functions

    \b
    Transform Functions:
      upper(s), lower(s)         Case conversion
      strip(s), lstrip(s), rstrip(s)  Whitespace removal
      replace(s, old, new)       String replacement
      lpad(s, width, char)       Left pad: lpad('7', 5, '0') → '00007'
      rpad(s, width, char)       Right pad: rpad('7', 5, '0') → '70000'
      substr(s, start, end)      Substring extraction
      concat(a, b, ...)          String concatenation (or use +)
      int(s), float(s), str(n)   Type conversion
      round(n, d), abs(n)        Numeric operations
      iif(cond, then, else)      Conditional (iif to avoid Python's if)
      coalesce(a, b, ...)        First non-null value
      isint(s), isfloat(s)       Check if text is numeric

    \b
    Operators: +, -, *, /, %, and, or, not

    Examples:

        # Prepend '0' to phone numbers without dots
        pipedrive-cli update -e per -b backup/ \\
          -f "not(contains(tel_s, '.'))" \\
          -s "tel_s='0' + tel_s"

        # Uppercase names
        pipedrive-cli update -e per -s "name=upper(name)"

        # Pad codes to 5 digits
        pipedrive-cli update -e deals -f "notnull(code)" -s "code=lpad(code, 5, '0')"

        # Multiple assignments
        pipedrive-cli update -e per \\
          -s "first_name=upper(first_name)" \\
          -s "last_name=upper(last_name)"

        # Dry-run with filter
        pipedrive-cli update -e per -b data/ -f "isint(code)" -s "code=lpad(code, 5, '0')" -n
    """
    # Resolve entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    # Get token for API mode
    token = None
    if not base:
        token = get_api_token()

    # Load fields for resolution
    if base:
        try:
            package = load_package(base)
            fields = get_entity_fields(package, matched_entity.name)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))

        if not fields:
            fields = []
    else:
        async def fetch_fields():
            async with PipedriveClient(token) as client:
                return await client.fetch_fields(matched_entity)

        if matched_entity.fields_endpoint:
            fields = asyncio.run(fetch_fields())
        else:
            fields = []

    # Build field lookup by key
    field_by_key: dict[str, dict] = {f.get("key", ""): f for f in fields}

    # Build set of field keys for validation
    field_keys = {f["key"] for f in fields}

    # Use interactive prompt for ambiguous fields unless quiet mode
    on_ambiguous = prompt_field_choice if not quiet else None

    # Resolve filter expression
    resolved_filter = None
    filter_resolutions: dict[str, tuple[str, str]] = {}
    if filter_expr:
        try:
            resolved_filter, filter_resolutions = resolve_filter_expression(
                fields, filter_expr, on_ambiguous=on_ambiguous
            )
        except AmbiguousMatchError as e:
            raise click.ClickException(f"Ambiguous field in filter: {e}")

        # Validate filter expression
        try:
            validate_expression(resolved_filter, field_keys)
        except FilterError as e:
            raise click.ClickException(str(e))

        # Show resolved filter (unless quiet)
        if not quiet:
            name_line, key_line = format_resolved_expression(
                filter_expr, resolved_filter, filter_resolutions
            )
            if key_line:
                console.print(f"[dim]Filter w/ names: {name_line}[/dim]")
                console.print(f"[dim]Filter w/ keys:  {key_line}[/dim]")
            else:
                console.print(f"[dim]Filter: {resolved_filter}[/dim]")

    # Resolve and display assignments
    # Format: [(target_key, original_expr, resolved_expr), ...]
    resolved_assignments: list[tuple[str, str, str]] = []

    for assignment in assignments:
        try:
            target_key, original_expr, resolved_expr, resolutions = resolve_assignment(
                fields, assignment, on_ambiguous=on_ambiguous
            )

            # Validate set expression
            validate_expression(resolved_expr, field_keys)

            resolved_assignments.append((target_key, original_expr, resolved_expr))

            # Show resolved assignment (unless quiet)
            if not quiet:
                # Get original field identifier from assignment
                original_field, _ = parse_assignment(assignment)
                name_line, key_line = format_resolved_assignment(
                    original_field, target_key, original_expr, resolved_expr, resolutions
                )
                if key_line:
                    console.print(f"[dim]Set w/ names:    {name_line}[/dim]")
                    console.print(f"[dim]Set w/ keys:     {key_line}[/dim]")
                else:
                    console.print(f"[dim]Set: {name_line}[/dim]")
        except ValueError as e:
            raise click.ClickException(str(e))
        except AmbiguousMatchError as e:
            raise click.ClickException(f"Ambiguous field in assignment: {e}")
        except FilterError as e:
            raise click.ClickException(str(e))

    if not quiet and (filter_expr or assignments):
        console.print()

    # Dry-run mode with no data operation
    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")
        console.print()

    # Load records
    if base:
        records = load_records(base, matched_entity.name)
        if not records:
            console.print(
                f"[yellow]No records found for '{matched_entity.name}' in {base}[/yellow]"
            )
            return
    else:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(f"Fetching {matched_entity.name}...", total=None)

            async def fetch_records():
                recs = []
                async with PipedriveClient(token) as client:
                    async for record in client.fetch_all(matched_entity):
                        recs.append(record)
                        progress.update(task, description=f"Fetched {len(recs)} records...")
                return recs

            records = asyncio.run(fetch_records())

        if not records:
            console.print(f"[yellow]No records found for '{matched_entity.name}'[/yellow]")
            return

    # Build option lookup for enum/set field comparison (used in filter and assignments)
    option_lookup = build_option_lookup(fields) if fields else {}

    # Apply filter
    if resolved_filter:
        try:
            filtered_records = [
                r for r in records
                if filter_record(preprocess_record_for_filter(r, option_lookup), resolved_filter)
            ]
        except FilterError as e:
            raise click.ClickException(str(e))
    else:
        filtered_records = records

    # Apply limit
    if limit and limit > 0:
        filtered_records = filtered_records[:limit]

    total_matching = len(filtered_records)
    console.print(f"[dim]Found {total_matching} matching record(s)[/dim]")

    if total_matching == 0:
        return

    # Apply updates
    if base:
        # Local mode: update in memory and save
        log_file = open(log, "w", encoding="utf-8") if log else None

        try:
            # Build assignment list for apply_update_local
            assignment_list = [
                (target_key, resolved_expr)
                for target_key, _, resolved_expr in resolved_assignments
            ]
            stats, changes = apply_update_local(
                filtered_records,
                assignment_list,
                dry_run=dry_run,
                option_lookup=option_lookup,
            )

            # Write log
            if log_file:
                for change in changes:
                    log_file.write(json.dumps(change, default=str) + "\n")
                if stats.errors:
                    for error in stats.errors:
                        log_file.write(json.dumps({"error": error}) + "\n")

            # Save records (if not dry-run)
            if not dry_run:
                save_records(base, matched_entity.name, records)

        finally:
            if log_file:
                log_file.close()

        console.print()

        if dry_run:
            console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        else:
            console.print("[green]Update completed![/green]")

        if log:
            console.print(f"[dim]Log written to:[/dim] {log}")

        # Show summary
        table = Table(title="Update Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green", justify="right")

        table.add_row("Total records", str(stats.total))
        table.add_row("Updated", str(stats.updated))
        table.add_row("Skipped (unchanged)", str(stats.skipped))
        table.add_row("Failed", str(stats.failed))

        console.print(table)

        # Show sample changes
        if changes and len(changes) <= 5:
            console.print()
            console.print("[dim]Changes:[/dim]")
            for change in changes:
                field_def = field_by_key.get(change["field"], {})
                field_name = field_def.get("name", change["field"])
                console.print(
                    f"  [cyan]#{change['id']}[/cyan] {field_name}: "
                    f"[red]{change['old']}[/red] → [green]{change['new']}[/green]"
                )
        elif changes:
            console.print()
            console.print(f"[dim]{len(changes)} field value(s) changed[/dim]")

        if stats.errors:
            console.print()
            console.print("[red]Errors:[/red]")
            for error in stats.errors[:5]:
                console.print(f"  [red]{error}[/red]")
            if len(stats.errors) > 5:
                console.print(f"  [dim]... and {len(stats.errors) - 5} more errors[/dim]")

    else:
        # API mode: update records via API
        log_file = open(log, "w", encoding="utf-8") if log else None
        updated_count = 0
        failed_count = 0
        errors: list[str] = []

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Updating records...", total=None)

                async def update_records():
                    nonlocal updated_count, failed_count, errors
                    async with PipedriveClient(token) as client:
                        for i, record in enumerate(filtered_records):
                            record_id = record.get("id")
                            progress.update(
                                task,
                                description=f"Updating {i+1}/{total_matching}..."
                            )

                            if dry_run:
                                # Just compute what would change
                                for target_key, _, resolved_expr in resolved_assignments:
                                    try:
                                        old_value = record.get(target_key)
                                        new_value = evaluate_assignment(record, resolved_expr)
                                        if new_value != old_value:
                                            updated_count += 1
                                            if log_file:
                                                log_file.write(json.dumps({
                                                    "id": record_id,
                                                    "field": target_key,
                                                    "old": old_value,
                                                    "new": new_value,
                                                    "action": "would_update",
                                                }, default=str) + "\n")
                                    except Exception as e:
                                        failed_count += 1
                                        errors.append(f"Record {record_id}: {e}")
                            else:
                                # Build update payload
                                update_payload: dict[str, Any] = {}
                                record_failed = False

                                for target_key, _, resolved_expr in resolved_assignments:
                                    try:
                                        new_value = evaluate_assignment(record, resolved_expr)
                                        update_payload[target_key] = new_value
                                    except Exception as e:
                                        record_failed = True
                                        failed_count += 1
                                        err = f"Record {record_id}, field {target_key}: {e}"
                                        errors.append(err)
                                        if log_file:
                                            log_file.write(json.dumps({
                                                "id": record_id,
                                                "field": target_key,
                                                "error": str(e),
                                            }) + "\n")

                                if update_payload and not record_failed:
                                    try:
                                        await client.update(
                                            matched_entity, record_id, update_payload
                                        )
                                        updated_count += 1
                                        if log_file:
                                            log_file.write(json.dumps({
                                                "id": record_id,
                                                "action": "updated",
                                                "fields": update_payload,
                                            }, default=str) + "\n")
                                    except Exception as e:
                                        failed_count += 1
                                        errors.append(f"Record {record_id}: {e}")
                                        if log_file:
                                            log_file.write(json.dumps({
                                                "id": record_id,
                                                "action": "failed",
                                                "error": str(e),
                                            }) + "\n")

                asyncio.run(update_records())

        finally:
            if log_file:
                log_file.close()

        console.print()

        if dry_run:
            console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
        else:
            console.print("[green]Update completed![/green]")

        if log:
            console.print(f"[dim]Log written to:[/dim] {log}")

        # Show summary
        table = Table(title="Update Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Count", style="green", justify="right")

        table.add_row("Total matching", str(total_matching))
        if dry_run:
            table.add_row("Would update", str(updated_count))
        else:
            table.add_row("Updated", str(updated_count))
        table.add_row("Failed", str(failed_count))

        console.print(table)

        if errors:
            console.print()
            console.print("[red]Errors:[/red]")
            for error in errors[:5]:
                console.print(f"  [red]{error}[/red]")
            if len(errors) > 5:
                console.print(f"  [dim]... and {len(errors) - 5} more errors[/dim]")


@record.command("delete")
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
    help="Delete records from local datapackage instead of API",
)
@click.option(
    "--filter",
    "-f",
    "filter_expr",
    default=None,
    help="Filter expression to select records to delete",
)
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Preview deletions without executing them",
)
@click.option(
    "--quiet",
    "-q",
    is_flag=True,
    help="Don't show resolved filter before results",
)
@click.option(
    "--log",
    "-l",
    type=click.Path(path_type=Path),
    default=None,
    help="Write detailed log to file (JSON lines format)",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Maximum number of records to delete",
)
@click.option(
    "--force",
    is_flag=True,
    help="Skip confirmation prompt",
)
@click.pass_context
def record_delete(
    ctx: click.Context,
    entity: str,
    base: Path | None,
    filter_expr: str | None,
    dry_run: bool,
    quiet: bool,
    log: Path | None,
    limit: int | None,
    force: bool,
) -> None:
    """Delete records matching a filter.

    Removes records from either a local datapackage (--base) or via Pipedrive API.
    Requires confirmation unless --force or --dry-run is used.

    \b
    Examples:
      # Preview deletions (dry-run)
      pipedrive-cli record delete -e per -b data/ -f "contains(name, 'TEST')" -n

      # Delete with confirmation
      pipedrive-cli record delete -e per -b data/ -f "isnull(email)"

      # Delete via API without confirmation
      pipedrive-cli record delete -e per -f "id == 12345" --force
    """
    # Match entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    # Require filter unless --force
    if not filter_expr and not force:
        raise click.UsageError("--filter is required (or use --force to delete all records)")

    # Load fields and resolve filter
    if base:
        # Local mode
        try:
            package = load_package(base)
        except FileNotFoundError as e:
            raise click.ClickException(str(e))

        fields = get_entity_fields(package, matched_entity.name)
    else:
        # API mode - get fields from API
        api_token = ctx.obj.get("api_token") if ctx.obj else None
        if not api_token:
            api_token = os.environ.get("PIPEDRIVE_API_TOKEN")
        if not api_token:
            raise click.ClickException("API token required (use --token or PIPEDRIVE_API_TOKEN)")

        async def get_api_fields() -> list[dict[str, Any]]:
            async with PipedriveClient(api_token) as client:
                return await client.get_fields(matched_entity)

        fields = asyncio.run(get_api_fields())

    # Resolve filter expression
    resolved_expr: str | None = None
    if filter_expr:
        try:
            resolved_expr, _ = resolve_filter_expression(fields, filter_expr)
        except AmbiguousMatchError as e:
            raise click.ClickException(f"Ambiguous field in filter: {e}")
        except Exception as e:
            raise click.ClickException(f"Failed to resolve filter: {e}")

        if not quiet:
            console.print(f"[dim]Filter: {resolved_expr}[/dim]")

        # Validate expression
        try:
            field_keys = {f.get("key", "") for f in fields}
            validate_expression(resolved_expr, field_keys)
        except FilterError as e:
            raise click.ClickException(str(e))

    # Build option lookup for enum/set preprocessing
    option_lookup = build_option_lookup(fields)

    # Load and filter records
    if base:
        # Local mode
        records = load_records(base, matched_entity.name)

        if resolved_expr:
            records_to_delete = [
                r for r in records
                if filter_record(preprocess_record_for_filter(r, option_lookup), resolved_expr)
            ]
        else:
            records_to_delete = records

        # Apply limit
        if limit and len(records_to_delete) > limit:
            records_to_delete = records_to_delete[:limit]

        # Check for empty result
        if not records_to_delete:
            console.print("[yellow]No records match filter. Nothing to delete.[/yellow]")
            return

        # Confirmation prompt
        if not dry_run and not force:
            count = len(records_to_delete)
            console.print(f"\n[bold]Will delete {count} {matched_entity.name}:[/bold]")
            for r in records_to_delete[:5]:
                record_id = r.get("id", "?")
                record_name = r.get("name") or r.get("title") or ""
                console.print(f"  - {record_id}: {record_name}")
            if len(records_to_delete) > 5:
                console.print(f"  [dim]... and {len(records_to_delete) - 5} more[/dim]")
            console.print()

            if not click.confirm("Proceed with deletion?"):
                console.print("[yellow]Aborted.[/yellow]")
                return

        # Execute deletion
        delete_ids = {r.get("id") for r in records_to_delete}
        remaining_records = [r for r in records if r.get("id") not in delete_ids]

        log_file: Any = None
        try:
            if log:
                log_file = open(log, "w")

            if not dry_run:
                if remaining_records:
                    save_records(base, matched_entity.name, remaining_records)
                else:
                    # All records deleted - write empty CSV with just headers
                    csv_path = base / f"{matched_entity.name}.csv"
                    fieldnames = list(records[0].keys()) if records else []
                    with open(csv_path, "w", newline="", encoding="utf-8") as f:
                        import csv as csv_module
                        writer = csv_module.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()

            # Write log
            if log_file:
                for r in records_to_delete:
                    log_file.write(json.dumps({
                        "action": "delete",
                        "id": r.get("id"),
                        "name": r.get("name") or r.get("title"),
                        "status": "deleted" if not dry_run else "dry-run",
                    }, default=str) + "\n")

        finally:
            if log_file:
                log_file.close()

        # Display summary
        console.print()
        count = len(records_to_delete)
        if dry_run:
            console.print(f"[yellow]DRY RUN - Would delete {count} records[/yellow]")
        else:
            console.print(f"[green]Deleted {count} records[/green]")

        if log:
            console.print(f"[dim]Log written to: {log}[/dim]")

    else:
        # API mode
        api_token = ctx.obj.get("api_token") if ctx.obj else None
        if not api_token:
            api_token = os.environ.get("PIPEDRIVE_API_TOKEN")

        async def delete_via_api() -> None:
            async with PipedriveClient(api_token) as client:
                # Fetch records
                all_records: list[dict[str, Any]] = []
                async for record in client.get_all(matched_entity):
                    all_records.append(record)

                # Filter records
                if resolved_expr:
                    records_to_del = [
                        r for r in all_records
                        if filter_record(
                            preprocess_record_for_filter(r, option_lookup), resolved_expr
                        )
                    ]
                else:
                    records_to_del = all_records

                # Apply limit
                if limit and len(records_to_del) > limit:
                    records_to_del = records_to_del[:limit]

                # Check for empty result
                if not records_to_del:
                    console.print("[yellow]No records match filter. Nothing to delete.[/yellow]")
                    return

                # Confirmation prompt
                if not dry_run and not force:
                    console.print(
                        f"\n[bold]Will delete {len(records_to_del)} {matched_entity.name}:[/bold]"
                    )
                    for r in records_to_del[:5]:
                        record_id = r.get("id", "?")
                        record_name = r.get("name") or r.get("title") or ""
                        console.print(f"  - {record_id}: {record_name}")
                    if len(records_to_del) > 5:
                        console.print(f"  [dim]... and {len(records_to_del) - 5} more[/dim]")
                    console.print()

                    if not click.confirm("Proceed with deletion?"):
                        console.print("[yellow]Aborted.[/yellow]")
                        return

                # Execute deletions sequentially
                deleted_count = 0
                failed_count = 0
                errors: list[str] = []

                log_file: Any = None
                try:
                    if log:
                        log_file = open(log, "w")

                    for r in records_to_del:
                        record_id = r.get("id")
                        record_name = r.get("name") or r.get("title") or ""

                        try:
                            if not dry_run:
                                await client.delete(matched_entity, record_id)
                            deleted_count += 1

                            if not quiet:
                                console.print(f"  Deleted {matched_entity.name}/{record_id}")

                            if log_file:
                                log_file.write(json.dumps({
                                    "action": "delete",
                                    "id": record_id,
                                    "name": record_name,
                                    "status": "deleted" if not dry_run else "dry-run",
                                }, default=str) + "\n")

                        except PipedriveError as e:
                            failed_count += 1
                            errors.append(f"{record_id}: {e}")
                            console.print(
                                f"  [red]Failed {matched_entity.name}/{record_id}: {e}[/red]"
                            )

                            if log_file:
                                log_file.write(json.dumps({
                                    "action": "delete",
                                    "id": record_id,
                                    "name": record_name,
                                    "status": "failed",
                                    "error": str(e),
                                }, default=str) + "\n")

                finally:
                    if log_file:
                        log_file.close()

                # Display summary
                console.print()
                if dry_run:
                    msg = f"[yellow]DRY RUN - Would delete {deleted_count} records[/yellow]"
                    console.print(msg)
                else:
                    console.print(f"[green]Deleted {deleted_count} records[/green]")

                if failed_count:
                    console.print(f"[red]Failed: {failed_count}[/red]")

                if log:
                    console.print(f"[dim]Log written to: {log}[/dim]")

        asyncio.run(delete_via_api())


# -----------------------------------------------------------------------------
# Schema commands
# -----------------------------------------------------------------------------


@main.group()
def schema() -> None:
    """Schema operations for datapackages."""
    pass


@schema.command(name="diff")
@click.argument("target", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("source", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "-e", "--entity",
    required=True,
    help="Entity to compare (e.g., persons, organizations, deals)",
)
@click.option(
    "-o", "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format",
)
def schema_diff(
    target: Path,
    source: Path,
    entity: str,
    output: str,
) -> None:
    """Compare field metadata between two datapackages.

    TARGET is the datapackage to check (e.g., local reference).
    SOURCE is the datapackage to compare against (e.g., fresh backup).

    Shows:
    - Fields in SOURCE but not in TARGET (candidates for merge)
    - CSV columns in TARGET without metadata
    - Fields in TARGET but not in SOURCE (local-only or deleted)
    """
    # Match entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    # Load both datapackages
    try:
        target_pkg = load_package(target)
        source_pkg = load_package(source)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    # Get field metadata from both
    target_fields = get_entity_fields(target_pkg, matched_entity.name)
    source_fields = get_entity_fields(source_pkg, matched_entity.name)

    # Get CSV columns from target
    target_csv_columns = get_csv_columns(target, matched_entity.name)

    # Compute diff
    diff = diff_field_metadata(target_fields, source_fields, target_csv_columns)

    if output == "json":
        # JSON output
        result = {
            "target": str(target),
            "source": str(source),
            "entity": matched_entity.name,
            "target_metadata_count": len(target_fields),
            "source_metadata_count": len(source_fields),
            "target_csv_columns": len(target_csv_columns),
            "in_source_only": diff["in_source_only"],
            "in_target_only": diff["in_target_only"],
            "in_csv_no_metadata": diff["in_csv_no_metadata"],
            "common": len(diff["common"]),
        }
        console.print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
    else:
        # Table output
        console.print(f"[bold]Comparing {matched_entity.name} fields:[/bold]")
        console.print(f"  TARGET: {target} ({len(target_fields)} fields in metadata, "
                      f"{len(target_csv_columns)} CSV columns)")
        console.print(f"  SOURCE: {source} ({len(source_fields)} fields in metadata)")
        console.print()

        # Fields in source only (merge candidates)
        in_source_only = diff["in_source_only"]
        # Filter to only show those that have data in CSV
        merge_candidates = [f for f in in_source_only if f.get("key") in target_csv_columns]
        not_in_csv = [f for f in in_source_only if f.get("key") not in target_csv_columns]

        if merge_candidates:
            console.print(f"[green]Fields in SOURCE with data in TARGET CSV "
                          f"(candidates for merge): {len(merge_candidates)}[/green]")
            for f in merge_candidates:
                key = f.get("key", "?")
                name = f.get("name", key)
                ftype = f.get("field_type", "?")
                short_key = key[:20] + "..." if len(key) > 20 else key
                console.print(f"  [green]+[/green] {short_key} ({name}) - {ftype}")
            console.print()

        if not_in_csv:
            console.print(f"[dim]Fields in SOURCE without data in TARGET CSV "
                          f"(deleted fields): {len(not_in_csv)}[/dim]")
            for f in not_in_csv[:5]:
                key = f.get("key", "?")
                name = f.get("name", key)
                short_key = key[:20] + "..." if len(key) > 20 else key
                console.print(f"  [dim]~[/dim] {short_key} ({name})")
            if len(not_in_csv) > 5:
                console.print(f"  [dim]... and {len(not_in_csv) - 5} more[/dim]")
            console.print()

        # CSV columns without metadata
        in_csv_no_meta = diff["in_csv_no_metadata"]
        if in_csv_no_meta:
            console.print(f"[yellow]CSV columns in TARGET without metadata: "
                          f"{len(in_csv_no_meta)}[/yellow]")
            for f in in_csv_no_meta[:10]:
                key = f.get("key", "?")
                short_key = key[:30] + "..." if len(key) > 30 else key
                console.print(f"  [yellow]![/yellow] {short_key}")
            if len(in_csv_no_meta) > 10:
                console.print(f"  [dim]... and {len(in_csv_no_meta) - 10} more[/dim]")
            console.print()

        # Fields only in target (local-only or deleted from Pipedrive)
        in_target_only = diff["in_target_only"]
        if in_target_only:
            console.print(f"[cyan]Fields in TARGET but not in SOURCE "
                          f"(local-only or deleted): {len(in_target_only)}[/cyan]")
            for f in in_target_only[:5]:
                key = f.get("key", "?")
                name = f.get("name", key)
                short_key = key[:20] + "..." if len(key) > 20 else key
                console.print(f"  [cyan]-[/cyan] {short_key} ({name})")
            if len(in_target_only) > 5:
                console.print(f"  [dim]... and {len(in_target_only) - 5} more[/dim]")
            console.print()

        # Summary
        console.print(f"[dim]Common fields: {len(diff['common'])}[/dim]")


@schema.command(name="merge")
@click.argument("target", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("source", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "-e", "--entity",
    required=True,
    help="Entity to merge (e.g., persons, organizations, deals)",
)
@click.option(
    "-o", "--output",
    type=click.Path(file_okay=False, path_type=Path),
    required=True,
    help="Output directory for merged datapackage (must not exist or use --force)",
)
@click.option(
    "-n", "--dry-run",
    is_flag=True,
    help="Preview changes without creating output",
)
@click.option(
    "--exclude",
    type=str,
    help="Comma-separated field keys to exclude from merge",
)
@click.option(
    "--include-only",
    type=str,
    help="Only merge these specific field keys (comma-separated)",
)
@click.option(
    "--force",
    is_flag=True,
    help="Allow overwriting existing output directory",
)
def schema_merge(
    target: Path,
    source: Path,
    entity: str,
    output: Path,
    dry_run: bool,
    exclude: str | None,
    include_only: str | None,
    force: bool,
) -> None:
    """Merge missing field metadata from SOURCE into a copy of TARGET.

    TARGET is the datapackage to enrich (will be copied, not modified).
    SOURCE is the datapackage providing additional field metadata.
    OUTPUT is where the merged datapackage will be created.

    Only fields that:
    - Exist in SOURCE but not in TARGET metadata
    - Have corresponding data in TARGET CSV
    - Are not excluded

    will be added. Existing fields in TARGET are never overwritten.
    """
    # Match entity prefix
    try:
        matched_entity = match_entity(entity)
    except (NoMatchError, AmbiguousMatchError) as e:
        raise click.ClickException(str(e))

    # Check output path
    if output == target:
        raise click.ClickException("Output path cannot be the same as target. "
                                   "Use a different path to avoid modifying the original.")

    if output.exists() and not force:
        raise click.ClickException(f"Output path '{output}' already exists. "
                                   f"Use --force to overwrite.")

    # Load both datapackages
    try:
        target_pkg = load_package(target)
        source_pkg = load_package(source)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    # Get field metadata from both
    target_fields = get_entity_fields(target_pkg, matched_entity.name)
    source_fields = get_entity_fields(source_pkg, matched_entity.name)

    # Get CSV columns from target
    target_csv_columns = get_csv_columns(target, matched_entity.name)

    # Parse exclude/include options
    exclude_keys: set[str] | None = None
    include_only_keys: set[str] | None = None

    if exclude:
        exclude_keys = {k.strip() for k in exclude.split(",")}
    if include_only:
        include_only_keys = {k.strip() for k in include_only.split(",")}

    # Compute merge
    merged_fields, added_fields = merge_field_metadata(
        target_fields,
        source_fields,
        target_csv_columns,
        exclude_keys=exclude_keys,
        include_only_keys=include_only_keys,
    )

    # Show what would be added
    console.print(f"[bold]Merging {matched_entity.name} metadata:[/bold]")
    console.print(f"  TARGET: {target} ({len(target_fields)} fields)")
    console.print(f"  SOURCE: {source} ({len(source_fields)} fields)")
    console.print(f"  OUTPUT: {output}")
    console.print()

    if not added_fields:
        console.print("[yellow]No fields to merge. All relevant metadata already exists "
                      "in TARGET or no source fields match the criteria.[/yellow]")
        return

    console.print(f"[green]Fields to add: {len(added_fields)}[/green]")
    for f in added_fields:
        key = f.get("key", "?")
        name = f.get("name", key)
        ftype = f.get("field_type", "?")
        short_key = key[:20] + "..." if len(key) > 20 else key
        console.print(f"  [green]+[/green] {short_key} ({name}) - {ftype}")
    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN - no changes made[/yellow]")
        return

    # Create output directory (copy of target)
    if output.exists() and force:
        shutil.rmtree(output)

    shutil.copytree(target, output)

    # Load the copied package and update fields
    output_pkg = load_package(output)
    update_entity_fields(output_pkg, matched_entity.name, merged_fields)
    save_package(output_pkg, output)

    console.print(f"[green]Merged datapackage created at:[/green] {output}")
    msg = f"Added {len(added_fields)} field(s) to {matched_entity.name} metadata"
    console.print(f"[dim]{msg}[/dim]")


if __name__ == "__main__":
    main()
