# Changelog

All notable changes to the PS4 Game Scraper project are documented here.

## [1.0.0] — 2026-02-07

### Added

#### Core Scraping
- Category page scraping with parallel batch fetching (15 pages at a time)
- RSS feed scraping as supplementary data source
- On-demand individual game page scraping with lock mechanism to prevent duplicate requests
- Smart link classification: Game, Update, DLC, Fix, Backport, Mod types
- Version extraction from URLs and page text (e.g., v1.56)
- Firmware requirement detection from page content
- CUSA code extraction (e.g., CUSA00411, CUSA00419)
- Pack description extraction for detailed link labeling
- Multi-host download link detection (Akira, Viking, 1Fichier, Mediafire, Rootz, Gofile, LetsUpload, Viki)
- Screenshot extraction with validation (filters out icons, avatars, ads, tracking pixels)
- Password extraction from game pages
- Screen language and guide text extraction

#### User Interface
- Responsive grid layout for game cards with cover images
- Game detail modal with cover art, info grid, description, screenshots, and organized download links
- Download links grouped by type (Game → Update → Fix → DLC) then by host
- Backport downloads grouped by firmware version (e.g., "Backport 5.xx Downloads")
- Configurable host display order with drag-to-reorder in settings
- Full-image modal for screenshot viewing
- Dark and Light theme support with CSS custom properties
- Fuzzy search powered by Fuse.js with fallback substring search
- Favorites system with persistent storage and filter toggle
- Sort toggle: Date (newest first) or Name (A-Z)
- Real-time progress display: games found, pages scanned, error count
- Animated progress spinner with completion state
- In-app notification banners (success, error, info)
- Desktop notifications on scan completion
- Discord community link in the UI
- Batched card rendering with `requestAnimationFrame` for large datasets

#### Settings
- Max games limit (0 = unlimited)
- Auto-scan on launch (when no cache exists)
- Theme selection (Dark / Light)
- Default sort order
- Cache auto-clear interval (configurable in days)
- Preferred host order with up/down reorder controls

#### Data Management
- Persistent cache via electron-store
- Separate storage for game data, favorites, and settings
- Cache expiry check on app load
- Manual cache clear with confirmation dialog

#### Error Handling
- Fetch error tracking with categories: timeouts, rate-limited, server errors, other
- Error summary display in status bar
- Retry button after failed scans
- Cancel button during active scans
- Modal retry/close on failed game page loads
- Graceful 404 handling (end-of-list detection)

#### Architecture
- Electron main process handles all HTTP requests (no CORS issues)
- Context isolation with preload bridge for security
- IPC handlers for store operations, external links, and scraping
- Scrape lock mechanism prevents duplicate concurrent requests for the same game
- Debounced search input (120ms)
- Lazy image loading with `loading="lazy"` and `decoding="async"`
- DocumentFragment-based DOM rendering for performance

### Technical Details
- Electron 28.x
- Cheerio for HTML parsing
- Axios with configurable timeouts (8s for listings, 30s for game pages)
- electron-store for persistent JSON storage
- electron-builder configured for Windows portable builds
- Fuse.js with 0.3 threshold for fuzzy matching