# Filter and Expression Reference

This document describes the filter and expression syntax used by `pipedrive-cli` commands.

## Overview

Expressions are used in two main contexts:

| Context | Command | Option | Purpose |
|---------|---------|--------|---------|
| **Filter** | `search`, `value update` | `-f, --filter` | Select records matching a condition |
| **Transform** | `value update` | `-s, --set` | Modify field values |

Both use [simpleeval](https://github.com/danthedeckie/simpleeval) syntax with extended functions.

## Field Resolution

Field identifiers in expressions are resolved automatically:

1. **Exact key match** - `abc123_status` matches exactly
2. **Key prefix match** - `abc123` matches `abc123_status` if unique
3. **Exact name match** - `Status` matches field with name "Status" (case-insensitive)
4. **Name prefix match** - `Stat` matches "Status" if unique

### Underscore Normalization

Underscores in identifiers are converted to spaces for name matching:
- `tel_s` matches field named "Tel standard"
- `first_name` matches field named "First name"

### Digit-Starting Keys

Pipedrive field keys are SHA-1 hashes that may start with digits (e.g., `25da23b938af...`). Since these are not valid Python identifiers, special handling is provided:

| User Input | Behavior | Example |
|------------|----------|---------|
| `25da` | Auto-detected (contains hex letter a-f) | `25da` → `_25da23b...` |
| `_25da` | Explicit escape (underscore prefix) | `_25da` → `25da23b...` |
| `_25` | Explicit escape for pure numeric prefix | `_25` → `25da23b...` |
| `25` | NOT resolved (treated as number literal) | `25` stays `25` |

**When to use explicit `_` prefix:**
- When the key prefix contains only digits (e.g., `25`, `123`)
- For clarity when referencing digit-starting keys

**Where digit-key escape works:**
- Filter expressions (`-f, --filter`)
- Transform expressions (`-s, --set`)
- Field include/exclude options (`-i, --include`, `-x, --exclude`)
- Field commands (`field delete`, `field copy`, etc.)

```bash
# Auto-detected: hex-like prefix with letters a-f
pipedrive-cli search -e per -f "25da != b85f"
# Filter: "Civilité-OLD" != Civilité

# Explicit escape: pure numeric prefix
pipedrive-cli search -e per -f "notnull(_25)"
# Filter: notnull("Civilité-OLD")

# Both fields with explicit escape
pipedrive-cli search -e per -f "_25da != _331"

# Include/exclude with digit-starting keys
pipedrive-cli search -e per -i "_25da,b85f,name" -o json

# Field commands with digit-starting keys
pipedrive-cli field delete -e per _25da
```

### Ambiguity Errors

If a prefix matches multiple fields, an error is raised:
```
Error: Ambiguous field 'tel': matches ['tel_mobile', 'tel_standard']
```

Use a longer prefix or the exact key to resolve ambiguity.

### Dry-Run Mode

Use `-n, --dry-run` to verify field resolution without executing:

```bash
pipedrive-cli search -e per -f "contains(First, 'test') and notnull(abc123)" -n
# Filter w/ names: contains("First name", 'test') and notnull("Custom Field")
# Filter w/ keys:  contains(first_name, 'test') and notnull(abc123_custom_field)
# (dry-run: search not executed)
```

## Operators

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `==` | Equal | `status == 'active'` |
| `!=` | Not equal | `status != 'closed'` |
| `>` | Greater than | `int(value) > 1000` |
| `<` | Less than | `int(value) < 500` |
| `>=` | Greater or equal | `int(age) >= 18` |
| `<=` | Less or equal | `int(score) <= 100` |

### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `and` | Logical AND | `notnull(email) and notnull(phone)` |
| `or` | Logical OR | `status == 'won' or status == 'lost'` |
| `not` | Logical NOT | `not(contains(name, 'test'))` |

### Arithmetic Operators (transform only)

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Addition / concatenation | `'0' + phone` or `int(a) + int(b)` |
| `-` | Subtraction | `int(total) - int(discount)` |
| `*` | Multiplication | `int(qty) * float(price)` |
| `/` | Division | `float(total) / float(count)` |
| `%` | Modulo | `int(value) % 10` |

## Type System

**No automatic type coercion.** Values from Pipedrive are typically strings. Use explicit conversion functions:

```bash
# Wrong - compares strings lexicographically
pipedrive-cli search -e deals -f "value > 1000"

# Correct - converts to integer first
pipedrive-cli search -e deals -f "int(value) > 1000"
```

## Functions

### String Matching (Filter)

Case-insensitive substring matching functions:

| Function | Description | Example |
|----------|-------------|---------|
| `contains(s, sub)` | True if `s` contains `sub` | `contains(name, 'john')` |
| `startswith(s, prefix)` | True if `s` starts with `prefix` | `startswith(email, 'info@')` |
| `endswith(s, suffix)` | True if `s` ends with `suffix` | `endswith(email, '.com')` |

### Null Checks

| Function | Description | Example |
|----------|-------------|---------|
| `isnull(s)` | True if `s` is None or empty string | `isnull(phone)` |
| `notnull(s)` | True if `s` is not None and not empty | `notnull(email)` |

### Type Checks

| Function | Description | Example |
|----------|-------------|---------|
| `isint(s)` | True if `s` is a valid integer | `isint(code)` |
| `isfloat(s)` | True if `s` is a valid float | `isfloat(price)` |
| `isnumeric(s)` | True if `s` is numeric (int or float) | `isnumeric(value)` |

### String Manipulation

| Function | Description | Example |
|----------|-------------|---------|
| `upper(s)` | Convert to uppercase | `upper(name)` |
| `lower(s)` | Convert to lowercase | `lower(email)` |
| `strip(s)` | Remove leading/trailing whitespace | `strip(name)` |
| `lstrip(s)` | Remove leading whitespace | `lstrip(code)` |
| `rstrip(s)` | Remove trailing whitespace | `rstrip(code)` |
| `replace(s, old, new)` | Replace all occurrences | `replace(phone, ' ', '')` |
| `substr(s, start, end)` | Extract substring (0-indexed) | `substr(code, 0, 3)` |
| `len(s)` | String length | `len(name) > 10` |
| `concat(a, b, ...)` | Concatenate strings | `concat(first, ' ', last)` |

### Padding

| Function | Description | Example |
|----------|-------------|---------|
| `lpad(s, width, char)` | Left-pad to width | `lpad('7', 5, '0')` → `'00007'` |
| `rpad(s, width, char)` | Right-pad to width | `rpad('7', 5, '0')` → `'70000'` |

### Type Conversion (Transform only)

| Function | Description | Example |
|----------|-------------|---------|
| `int(s)` | Convert to integer | `int('42')` → `42` |
| `float(s)` | Convert to float | `float('3.14')` → `3.14` |
| `str(n)` | Convert to string | `str(42)` → `'42'` |

### Numeric Operations (Transform only)

| Function | Description | Example |
|----------|-------------|---------|
| `round(n, d)` | Round to `d` decimal places | `round(3.14159, 2)` → `3.14` |
| `abs(n)` | Absolute value | `abs(-5)` → `5` |

### Conditional (Transform only)

| Function | Description | Example |
|----------|-------------|---------|
| `iif(cond, then, else)` | Conditional expression | `iif(notnull(a), a, b)` |
| `coalesce(a, b, ...)` | First non-null value | `coalesce(mobile, phone, '')` |

**Note:** Use `iif` instead of `if` to avoid conflict with Python's keyword.

### Constants

| Constant | Description | Example |
|----------|-------------|---------|
| `null` | Null value | `iif(isnull(x), null, x)` |
| `True` | Boolean true | `active == True` |
| `False` | Boolean false | `verified == False` |

## Command Reference

### search

Search and filter records with optional field selection.

```bash
pipedrive-cli search -e ENTITY [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-e, --entity` | Entity type (supports prefix matching) |
| `-b, --base PATH` | Search in local datapackage instead of API |
| `-f, --filter EXPR` | Filter expression |
| `-i, --include FIELDS` | Comma-separated field prefixes to include |
| `-x, --exclude FIELDS` | Comma-separated field prefixes to exclude |
| `-o, --format FORMAT` | Output: `table` (default), `json`, `csv` |
| `-l, --limit N` | Maximum records to return |
| `-n, --dry-run` | Show resolved filter only, don't execute |
| `-q, --quiet` | Don't show resolved filter before results |

### value update

Update field values on records matching a filter.

```bash
pipedrive-cli value update -e ENTITY -s ASSIGNMENT [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-e, --entity` | Entity type (supports prefix matching) |
| `-b, --base PATH` | Update local datapackage instead of API |
| `-f, --filter EXPR` | Filter expression to select records |
| `-s, --set ASSIGN` | Field assignment `field=expr` (repeatable) |
| `-n, --dry-run` | Preview changes without applying |
| `-q, --quiet` | Don't show resolved expressions |
| `-l, --log FILE` | Write detailed log (JSON lines) |
| `--limit N` | Maximum records to update |

## Examples

### Filter Examples

```bash
# Simple string search
pipedrive-cli search -e persons -f "contains(name, 'Smith')"

# Combine conditions
pipedrive-cli search -e persons -f "contains(name, 'John') and notnull(email)"

# Numeric comparison (with explicit conversion)
pipedrive-cli search -e deals -f "int(value) > 10000"

# Check for empty fields
pipedrive-cli search -e persons -f "isnull(phone)"

# String prefix matching
pipedrive-cli search -e organizations -f "startswith(name, 'Acme')"

# Using field name prefix (underscore = space)
pipedrive-cli search -e persons -f "notnull(tel_standard)"
# Resolves to: notnull("Tel standard")

# Check if text is numeric before comparison
pipedrive-cli search -e persons -f "isint(age) and int(age) >= 18"

# Output as JSON, suppress filter display
pipedrive-cli search -e deals -f "status == 'won'" -o json -q

# Field selection with filter
pipedrive-cli search -e persons -f "notnull(email)" -i "id,name,email" -l 10
```

### Transform Examples

```bash
# Prepend character to field
pipedrive-cli value update -e persons -b backup/ \
  -f "not(contains(phone, '.'))" \
  -s "phone='0' + phone"

# Uppercase names
pipedrive-cli value update -e persons \
  -s "name=upper(name)"

# Pad codes to fixed width
pipedrive-cli value update -e deals \
  -f "notnull(code) and isint(code)" \
  -s "code=lpad(code, 5, '0')"

# Multiple field updates
pipedrive-cli value update -e persons \
  -s "first_name=upper(first_name)" \
  -s "last_name=upper(last_name)"

# Conditional update
pipedrive-cli value update -e persons \
  -s "phone=iif(startswith(phone, '0'), phone, '0' + phone)"

# Replace characters
pipedrive-cli value update -e persons \
  -s "phone=replace(phone, ' ', '')"

# Trim whitespace
pipedrive-cli value update -e organizations \
  -s "name=strip(name)"

# Use coalesce for fallback values
pipedrive-cli value update -e persons \
  -s "display_phone=coalesce(mobile, phone, 'N/A')"

# Dry-run to preview changes
pipedrive-cli value update -e persons -b data/ \
  -f "isint(code)" \
  -s "code=lpad(code, 5, '0')" \
  -n

# Log changes to file
pipedrive-cli value update -e persons \
  -s "name=upper(name)" \
  -l changes.jsonl
```

### Field Resolution Examples

```bash
# Using key prefix
pipedrive-cli search -e per -f "notnull(abc123)"
# Resolves abc123 → abc123_custom_field

# Using name prefix
pipedrive-cli search -e per -f "notnull(First)"
# Resolves First → first_name (field named "First name")

# Underscore to space conversion
pipedrive-cli search -e per -f "notnull(tel_standard)"
# Resolves tel_standard → the field named "Tel standard"

# Digit-starting key prefix (auto-detected with hex letters)
pipedrive-cli search -e per -f "25da != b85f"
# Resolves 25da → _25da23b... and b85f → b85f32...

# Digit-starting key with explicit _ prefix
pipedrive-cli search -e per -f "notnull(_25)"
# Resolves _25 → 25da23b... (matches key starting with "25")

# Include digit-starting fields in output
pipedrive-cli search -e per -i "_25da,b85f,name" -o json
# Includes fields: 25da23b..., b85f32..., name

# Verify resolution with dry-run
pipedrive-cli search -e persons \
  -f "contains(First, 'test') and notnull(custom)" \
  -n
# Output:
# Filter w/ names: contains("First name", 'test') and notnull("Custom Field")
# Filter w/ keys:  contains(first_name, 'test') and notnull(abc123_custom_field)
# (dry-run: search not executed)
```

## Null Handling

### In Filters

- `isnull(field)` returns `True` for `None` or empty string `""`
- `notnull(field)` returns `True` for non-empty values
- String functions on `None` return `False` or empty results

### In Transforms

String functions preserve `None` values:
- `upper(None)` returns `None` (not `""`)
- `strip(None)` returns `None`

Use `coalesce()` to provide defaults:
```bash
pipedrive-cli value update -e persons \
  -s "phone=coalesce(strip(phone), 'N/A')"
```

## Error Handling

### Common Errors

**Ambiguous field match:**
```
Error: Ambiguous field 'tel': matches ['tel_mobile', 'tel_standard']
```
Solution: Use a longer prefix or exact key.

**Invalid expression syntax:**
```
Error: Invalid expression: unexpected token
```
Solution: Check quoting and operator syntax.

**Assignment without comparison:**
```
Error: Assignment '=' not allowed (use '==' for comparison)
```
Solution: Use `==` for equality comparison in filters.

**Type mismatch:**
```
Error: Filter evaluation error: '>' not supported between instances of 'str' and 'int'
```
Solution: Use explicit type conversion: `int(field) > 100`

## Best Practices

1. **Always use dry-run first** (`-n`) to verify field resolution
2. **Use explicit type conversion** for numeric comparisons
3. **Check for null/empty** before operations that might fail on null
4. **Use logging** (`-l`) for bulk updates to track changes
5. **Prefer exact keys** for production scripts to avoid ambiguity
6. **Quote strings** with single quotes: `'value'`
