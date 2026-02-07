# PS4 Game Scraper

A desktop application built with Electron that scrapes and catalogs PS4 games from dlpsgame.com, providing an organized interface to browse, search, and access download links from multiple hosting providers.

## Features

- **Game Catalog Scraping** — Automatically scrapes PS4 game listings from dlpsgame.com via category pages and RSS feeds
- **Multi-Host Download Links** — Organizes download links by host (Akira, Viking, 1Fichier, Mediafire, Rootz, and more)
- **Smart Link Classification** — Automatically categorizes links as Game, Update, DLC, Fix, Backport, or Mod
- **Version & Firmware Detection** — Extracts game versions (e.g., v1.56) and firmware requirements from page content
- **Fuzzy Search** — Powered by Fuse.js for fast, typo-tolerant game searching
- **Favorites System** — Star games to quickly access them later
- **Persistent Cache** — Games are cached locally via electron-store so you don't need to re-scrape every time
- **Game Detail Modal** — View cover art, description, screenshots, firmware info, and organized download links
- **Dark & Light Themes** — Toggle between dark and light UI themes
- **Configurable Settings**:
  - Max games to fetch (0 = unlimited)
  - Auto-scan on launch
  - Default sort order (Date or Name)
  - Cache auto-clear interval (days)
  - Preferred host download order (drag to reorder)
- **Progress Tracking** — Real-time display of games found, pages scanned, and errors during scraping
- **Desktop Notifications** — Get notified when scans complete
- **Responsive Design** — Adapts from mobile to ultra-wide displays (2400px+)
- **Error Resilience** — Tracks timeouts, rate limits, and server errors with retry capability

## Tech Stack

- **Electron** — Desktop app framework
- **Cheerio** — HTML parsing and scraping
- **Axios** — HTTP requests with timeout support
- **Fuse.js** — Fuzzy search library
- **electron-store** — Persistent JSON storage
- **electron-builder** — App packaging

## Prerequisites

- [Node.js](https://nodejs.org/) (v16 or higher)
- npm (comes with Node.js)

## Installation

```bash
# Clone or download the project
cd ps4-game-scraper

# Install dependencies
npm install
```

## Usage

### Development

```bash
npm start
```

### Build Portable Executable (Windows)

```bash
npm run build
```

This creates a portable `.exe` file in the `dist/` directory.

## Project Structure

```
ps4-game-scraper/
├── main.js          # Electron main process — IPC handlers, scraping logic
├── preload.js       # Context bridge — exposes safe API to renderer
├── renderer.js      # UI logic — DOM manipulation, state management
├── index.html       # App UI with embedded CSS
├── package.json     # Dependencies and build config
└── assets/
    └── icon1.ico    # App icon
```

## How It Works

1. **Category Scraping** — Fetches paginated game listings from `/category/ps4/` in parallel batches of 15 pages
2. **RSS Supplementation** — Fetches the RSS feed to fill in missing cover images and dates
3. **On-Demand Detail Scraping** — When you click a game card, the app scrapes the individual game page to extract:
   - Download links grouped by host provider
   - Link types (game, update, DLC, fix, backport, mod)
   - Game version and firmware requirements
   - Description, screenshots, voice/subtitle languages
   - Password information
4. **Caching** — All scraped data is persisted to disk via electron-store

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Escape` | Close the topmost open modal |

## Settings

Access settings via the ⚙ button in the toolbar:

| Setting | Description | Default |
|---------|-------------|---------|
| Max Games | Limit number of games to fetch (0 = unlimited) | 0 |
| Auto-scan | Start scanning on launch if no cache exists | Off |
| Theme | Dark or Light | Dark |
| Default Sort | Date (newest first) or Name (A-Z) | Date |
| Cache Auto-Clear | Auto-clear cache after N days (0 = disabled) | 0 |
| Host Order | Drag to set preferred download host priority | Default |

## Supported Download Hosts

| Host | ID |
|------|----|
| Akira (akirabox.com) | `akira` |
| Viking (vikingfile.com) | `viking` |
| 1Fichier (1fichier.com) | `onefichier` |
| LetsUpload | `letsupload` |
| Mediafire | `mediafire` |
| Gofile | `gofile` |
| Rootz | `rootz` |
| Viki | `viki` |

## License

This project is for personal/educational use only.

## Discord

Join the community: [Discord Server](https://discord.gg/wp3WpWXP77)