# Release Process

## Steps

### 1. Create Version Bump PR

Update version in both files:
- [`setup.py`](setup.py#L44): `package_version = "1.12.0"`
- [`dbt/adapters/starrocks/__version__.py`](dbt/adapters/starrocks/__version__.py#L16): `version = "1.12.0"`

Move `[Unreleased]` items in [`CHANGELOG.md`](CHANGELOG.md) to new version section.

### 2. Merge PR to main

Ensure CI passes before merging.

### 3. Run Release Workflow

Go to **Actions → Release → Run workflow**

**Dry-run (recommended first):**
- Check "Dry run" 
- Verify output

**Actual release:**
- Uncheck "Dry run"
- Workflow publishes to PyPI and creates GitHub release

## The workflow will fail if:
- CI hasn't passed
- Version mismatch between files
- No changelog entry exists for version
- Tag already exists
