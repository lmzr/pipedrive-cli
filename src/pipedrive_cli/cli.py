"""Command-line interface for pipedrive-cli."""

import asyncio
import os
from datetime import datetime
from pathlib import Path

import click
from frictionless import Package, validate
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from . import __version__
from .backup import create_backup, describe_schemas
from .config import ENTITIES, RESTORE_ORDER
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
    type=click.Choice(list(ENTITIES.keys())),
    help="Specific entities to export (default: all)",
)
def backup(output: Path | None, entities: tuple[str, ...]) -> None:
    """Create a full backup of Pipedrive data as a datapackage."""
    token = get_api_token()

    if output is None:
        base = Path(f"backup-{datetime.now().strftime('%Y-%m-%d')}")
        output = get_unique_output_dir(base)

    entity_list = list(entities) if entities else None

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
    console.print(f"[green]Backup created successfully![/green]")
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
def describe() -> None:
    """Show field schemas from Pipedrive API."""
    token = get_api_token()

    console.print("[bold]Fetching schemas from Pipedrive...[/bold]")

    schemas = asyncio.run(describe_schemas(token))

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
    package_path = path / "datapackage.json" if path.is_dir() else path

    if not package_path.exists():
        raise click.ClickException(f"datapackage.json not found in {path}")

    console.print(f"[bold]Validating:[/bold] {package_path}")

    package = Package(str(package_path))
    report = validate(package)

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


@main.command()
@click.argument("path", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--dry-run",
    "-n",
    is_flag=True,
    help="Show what would be restored without making changes",
)
@click.option(
    "--entities",
    "-e",
    multiple=True,
    type=click.Choice(RESTORE_ORDER),
    help="Specific entities to restore (default: all)",
)
@click.option(
    "--log",
    "-l",
    type=click.Path(path_type=Path),
    default=None,
    help="Write detailed log to file (JSON lines format)",
)
def restore(path: Path, dry_run: bool, entities: tuple[str, ...], log: Path | None) -> None:
    """Restore a backup to Pipedrive.

    PATH is the backup directory containing datapackage.json.
    """
    token = get_api_token()

    if dry_run:
        console.print("[yellow]DRY RUN - no changes will be made[/yellow]")

    console.print(f"[bold]Restoring from:[/bold] {path}")

    entity_list = list(entities) if entities else None
    log_file = open(log, "w", encoding="utf-8") if log else None

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Starting restore...", total=None)

            def update_progress(message: str) -> None:
                progress.update(task, description=message)

            all_stats = asyncio.run(
                restore_backup(
                    token,
                    path,
                    entities=entity_list,
                    dry_run=dry_run,
                    log_file=log_file,
                    progress_callback=update_progress,
                )
            )

            progress.update(task, description="Restore complete!")

    finally:
        if log_file:
            log_file.close()

    console.print()

    if dry_run:
        console.print("[yellow]DRY RUN complete - no changes were made[/yellow]")
    else:
        console.print("[green]Restore completed![/green]")

    if log:
        console.print(f"[dim]Log written to:[/dim] {log}")

    # Show summary table
    table = Table(title="Restore Summary")
    table.add_column("Entity", style="cyan")
    table.add_column("Updated", style="blue", justify="right")
    table.add_column("Created", style="green", justify="right")
    table.add_column("Failed", style="red", justify="right")

    total_updated = 0
    total_created = 0
    total_failed = 0

    for entity_name, stats in all_stats.items():
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


if __name__ == "__main__":
    main()
