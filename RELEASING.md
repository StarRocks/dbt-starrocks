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

Go to **Actions → Release → Run workflow**, pick a **Release target**:

1. **`dry-run`** (default) — builds the sdist + wheel, prints the release notes preview. No publishing, no tag, no GitHub release.
2. **`test-pypi`** — publishes to [TestPyPI](https://test.pypi.org/) so you can `pip install -i https://test.pypi.org/simple/ dbt-starrocks==<version>` and smoke-test. No tag, no GitHub release. Requires the `TEST_PYPI_API_TOKEN` repo secret.
3. **`production`** — publishes to PyPI, creates the `v<version>` git tag, and publishes the GitHub release. Requires the `PYPI_API_TOKEN` repo secret.

Recommended flow: `dry-run` → `test-pypi` → install from TestPyPI and verify → `production`.

## The workflow will fail if:
- CI hasn't passed
- Version mismatch between files
- No changelog entry exists for version
- Tag already exists
