# Changelog

All notable changes to the Photo Organizer and Deduplicator project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1-00017] - 2025-12-03

### Added
- **Persistent Statistics Tracking System**
  - Implemented JSON-based statistics file (`Photo_Organizer_Statistics.json`) to track file operations
  - Statistics persist across application restarts
  - Tracks the following metrics:
    - Total number of files moved to destination
    - Total bytes moved to destination
    - Total number of files moved to duplicates folder
    - Total bytes moved to duplicates folder
    - Total number of files deleted (duplicates)
    - Total bytes deleted (duplicates)
  - Statistics are automatically loaded on startup
  - Statistics are saved periodically (every 10 operations or 30 seconds timeout, whichever comes first)
  - File locking support for thread-safe statistics updates on Unix/Linux systems

- **Statistics Adjustment Logic**
  - When files are moved from destination folder to duplicates folder, statistics are automatically adjusted:
    - Subtracts from destination statistics
    - Adds to duplicates statistics
  - Ensures accurate tracking when files are reorganized

### Changed
- **Log File Naming**
  - `Photo_Organizer.log` → `Photo_Organizer_Activities.log` (file operation activities)
  - `System.log` → `Photo_Organizer_Application.log` (system/application events)
  - `statistics.json` → `Photo_Organizer_Statistics.json` (statistics tracking)

- **Statistics Save Timing**
  - Statistics now save after every 10 operations OR after 30 seconds timeout (whichever comes first)
  - Improves data persistence and reduces risk of data loss on unexpected shutdowns

- **Application Log Cleanup**
  - Removed detailed statistics breakdown from `Photo_Organizer_Application.log`
  - Statistics are still saved to `Photo_Organizer_Statistics.json` but no longer clutter the application log
  - Application log now focuses on system events and service status

### Fixed
- Statistics now accurately reflect files moved between folders
- When files move from destination to duplicates, destination statistics are properly decremented
- **Mutagen dependency detection**: Fixed false warning about mutagen not being installed
  - Updated dependency check to import mutagen directly instead of relying on module-level flag
  - Properly detects mutagen when installed in virtual environment
- **Video metadata extraction**: Fixed MOV file metadata extraction
  - Removed dependency on non-existent `mutagen.quicktime` module (not available in mutagen 1.47.0+)
  - MOV files now use MP4 module since they share the same container format
  - Video metadata extraction works correctly for both MP4 and MOV files

### Technical Details
- Statistics are stored in JSON format for human readability and easy backup
- Statistics file location: `{DEST_DIR}/Photo_Organizer_Statistics.json`
- Statistics are saved with file locking on Unix/Linux systems for thread safety
- Backward compatible with existing `bytes_moved` and `bytes_deleted` legacy variables

---

## Previous Versions

For changes in previous versions, please refer to the git history or project documentation.

