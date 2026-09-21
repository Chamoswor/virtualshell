### Page: Changelog

```
## Unreleased
- Windows PowerShell 5.1 support with the full feature set (sync/async execution,
  scripts, session snapshots, zero-copy bridge, proxies, generate_psobject)
- New `powershell_edition` option ("auto" | "core" | "desktop") on `Shell` and the
  C++ `Config`; "auto" prefers pwsh and falls back to Windows PowerShell on Windows
- New `Shell.edition`, `Shell.powershell_version`, `Shell.powershell_path` and
  `Shell.configured_edition` properties; C++ `getPowerShellEdition()` /
  `getResolvedPowerShellPath()`
- `Config.powershell_path` now defaults to "" (resolved from the edition at start)
  instead of "pwsh"
- Integration tests run once per installed PowerShell edition

## 1.1.0
- Added per-command timeouts for async and batch commands
- New and improved wrapper api (implemented overloads for run* and script* methods)
- Internal improvements to command state management
- Ensured thread-safe restart handling, preventing race conditions during shell restarts
- Improved error handling and reporting in the C++ core
- Implemented optional debug logging in the C++ core for easier troubleshooting
- Updated documentation to reflect new features and changes
- Fixed various minor bugs and improved overall stability
- Enhanced test coverage for new features

## 1.0.2
- Wrapper fixes: improved error translation

## 1.0.1
- Initial public release with prebuilt wheels