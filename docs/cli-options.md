# CLI Short Options Convention

Reference document for consistent CLI option naming across all commands.

## Short Options Table

| Option | Long form | Usage | Notes |
|--------|-----------|-------|-------|
| `-e` | `--entity` | Entity type | Required for most commands |
| `-b` | `--base` | Local datapackage path | Optional, default=API |
| `-n` | `--dry-run` | Preview mode | No changes |
| `-o` | `--output` | Output file path | For file output |
| `-l` | `--log` | Log file path | JSON lines log |
| `-f` | `--field` / `--filter` | Field key / Filter expr | Context-dependent |
| `-t` | `--type` | Field type | For field create |
| `-i` | `--input` | Input file | For record import |
| `-k` | `--key` | Deduplication key | For record import |
| `-s` | `--sheet` | XLSX sheet name | For XLSX operations |
| `-q` | `--quiet` | Suppress verbose output | |
| `-r` | `--header-row` | Header row number | For XLSX |
| `-x` | `--exchange` / `--exclude` | Exchange names / Exclude fields | Context-dependent |

## Usage Guidelines

1. **Consistency**: When adding a new command, reuse existing short options for similar purposes
2. **Context-dependent options**: `-f` means `--field` in field commands, `--filter` in search/update
3. **Reserved options**: `-h` is reserved for `--help` by Click
4. **Long-only options**: Use long form only for rarely-used options (e.g., `--on-duplicate`, `--auto-id`, `--preserve-links`, `--force`, `--custom-only`)

## Commands by Option

### Common options (most commands)
- `-e`, `-b`, `-n`

### Field commands
- `field list`: `-e`, `-b`, `--custom-only`
- `field create`: `-e`, `-b`, `-t`, `-o` (options), `-n`
- `field copy`: `-e`, `-b`, `-f`, `-t`, `--transform`, `-x`, `-n`
- `field rename`: `-e`, `-b`, `-f`, `-o` (new name), `-n`
- `field delete`: `-e`, `-b`, `--force`, `-n`
- `field options list`: `-e`, `-b`, `-f`, `--show-usage`
- `field options add/remove/sync`: `-e`, `-b`, `-f`, `-n`, `--force`

### Record Operations
- `record search`: `-e`, `-b`, `-f`, `-i`, `-x`, `-o`, `-l`, `-n`, `-q`
- `record update`: `-e`, `-b`, `-f`, `-s`, `-n`, `-q`, `-l`, `--limit`
- `record import`: `-e`, `-b`, `-i`, `-k`, `-s`, `-n`, `-l`, `-q`, `--on-duplicate`, `--auto-id`

### Data operations
- `data convert`: `-o`, `-s`, `-r`, `--preserve-links`

### Backup & Restore
- `backup`: `-o`, `-e`, `-n`
- `store`: `-n`, `-e`, `-l`, `--no-update-base`
