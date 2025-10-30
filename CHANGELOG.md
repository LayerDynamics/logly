# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- GitHub Actions workflows for automated Linux and macOS releases
- MANIFEST.in for proper package distribution
- Comprehensive release notes generation
- Multi-architecture support (x86_64 and ARM64 for macOS)
- LaunchAgent/LaunchDaemon plist for macOS auto-start

## [0.1.0] - 2025-01-30

### Added
- Initial release of Logly
- System metrics collection (CPU, memory, disk, load average)
- Network activity monitoring via /proc/net parsing
- Log event parsing (fail2ban, syslog, auth.log, nginx, Django)
- SQLite-based time-series storage with optimized indexing
- Proactive issue detection system
- Query builder with fluent interface
- Health monitoring and scoring (0-100 scale)
- Event tracing with causality tracking
- IP reputation and threat intelligence
- Error pattern analysis
- CSV and JSON export capabilities
- Human-readable report generation
- CLI with comprehensive commands
- Systemd service integration for Linux
- Control script (logly.sh) for easy management
- Configurable data retention policies
- Automatic hourly/daily aggregation
- Test suite (unit, integration, e2e)
- Conda environment support

### Features
- **Minimal Dependencies**: Only requires PyYAML for runtime
- **Zero-Config Storage**: Automatic SQLite schema initialization
- **Direct /proc Parsing**: No external tools needed for metrics
- **Time-Series Optimization**: Indexed queries with aggregates
- **Thread-Safe**: Concurrent collection with proper locking
- **Hardcoded Paths**: Predictable file locations for security
- **Test Mode**: Environment variable for testing with temp directories

### Technical Details
- Python 3.8+ support
- Modular architecture with clear separation of concerns
- Collector pattern for extensible data collection
- Repository pattern for clean database abstraction
- Fluent builder pattern for complex queries
- Strategy pattern for multiple export formats

## Release Types

### Stable Releases (vX.Y.Z)
- Fully tested production-ready releases
- Semantic versioning
- Complete documentation
- Migration guides for breaking changes

### Pre-releases
- **Alpha** (vX.Y.Z-alpha.N): Early testing, may have bugs
- **Beta** (vX.Y.Z-beta.N): Feature complete, testing phase
- **RC** (vX.Y.Z-rc.N): Release candidate, final testing

[Unreleased]: https://github.com/ryanoboyle/logly/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/ryanoboyle/logly/releases/tag/v0.1.0
