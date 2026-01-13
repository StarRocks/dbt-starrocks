# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- `get_catalog` macro (#89)

## [1.11.0] - 2025-10-16

### Added
- CI Pipeline (#84)
- Apache License 2.0 (#85)
- Support for external materializations (#79)

### Changed
- Updated dbt support version (#82)

### Fixed
- Columns description (#83)

## [1.10.1] - 2025-09-11

### Fixed
- `add_begin_query` method issue in connections.py for StarRocks 3.5.0+ (#74)

## [1.10.0] - 2025-06-04

### Added
- `auth_plugin` parameter to dbt-starrocks (#69)
- Support for table as a possible table type option (#77)

### Changed
- Adjusted SQL hint position for incremental materialization (#67)

## [1.9.0] - 2025-03-20

### Added
- Support for `microbatch` incremental strategy on dbt-core 1.9.0 (#62)
- Submittable ETL task (#63)
- `incremental_strategy` configuration with support for dynamic overwrites (#59)

### Fixed
- Python 3.9.x compatibility problem (#66)

## [1.7.0] - 2024-10-14

### Added
- Support for Python 3.12
- Allow `unique_id` to take a list (#55)

### Fixed
- Parameter issues when executing the `dbt docs generate` command (#54)

## [1.6.3] - 2024-07-26

### Added
- Automatic bucketing when no buckets are specified (#50)

### Changed
- Updated to dbt-core 1.8.4 (#51)

### Fixed
- Other connection exceptions (#52)

## [1.6.2] - 2024-04-14

### Added
- Enable C extensions (#46)
- Support for complex types when adding columns (#43)

### Fixed
- JSONDecodeError import (#40)

## [1.6.1] - 2024-04-02

### Added
- Support for different ssl_mode options (#36)
- Basic unit tests

### Changed
- Upgraded to dbt-core 1.6.10
- Updated GitHub repository location

### Fixed
- Issue with parsing version number when version not defined in profiles.yml
- Cast from str to int issue
- MySQL connector errors (Unread result found)
- Server version detection issue
- CTE relation type handling
- Better defaults for testing
