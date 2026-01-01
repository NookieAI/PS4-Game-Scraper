❤️ Donate to the project: https://ko-fi.com/nookie_65120 🙏

<img width="943" height="467" alt="Screenshot 2026-01-01 185212" src="https://github.com/user-attachments/assets/45fa3088-05c5-4bc6-97d8-21e9f0f64659" />

# PS4 Game Scraper

A powerful, user-friendly desktop application for scraping and browsing PS4 games from dlpsgame.com. Built with Electron, this app offers fast scanning, offline caching, lazy loading, and support for multiple download hosts, making it easy to discover and download PS4 games.

## Features

- **Efficient Scanning**:
  - Concurrently fetches game lists from category pages and RSS feeds for quick results.
  - Displays progress with real-time game count (e.g., "Found 150 games so far").
  - Handles cancellations and retries.

- **Offline Capability**:
  - Caches all game data (titles, covers, dates, descriptions, screenshots) in localStorage.
  - Loads cached games instantly on startup—no internet required after initial scan.
  - Favorites persist across sessions.

- **Lazy Loading for Bandwidth Savings**:
  - Game lists are fetched upfront, but download links (Akira, Viking, MediaFire, 1Fichier) are scraped only when you click a game card.
  - Avoids unnecessary network requests for games you don't view.

- **Multiple Download Hosts**:
  - Akira (akirabox.com)
  - Viking (vikingfile.com)
  - MediaFire (mediafire.com) – links displayed in short format (e.g., https://www.mediafire.com/file/XXXXXXX)
  - 1Fichier (1fichier.com)

- **User-Friendly Interface**:
  - Dark theme with responsive grid layout.
  - Game cards show covers, titles, voice/subtitles/notes/size.
  - Modal details: full description, up to 2 screenshots (click to enlarge), and organized link lists with underlined headers.
  - Links separated into "Game" and "Updates" sections, with versions displayed (e.g., v1.00).
  - Star icon for favorites (yellow when favorited).
  - Search bar for filtering games by title.
  - Sort by date (newest first) or name (A-Z).
  - Confirmation dialog for cache clearing.

- **Additional Features**:
  - Desktop notifications for scan completion/cancellation.
  - External links open in default browser.
  - ESC key or click outside to close modals.
  - Discord link for support (Nookie_65120).
  - Portable exe build for easy sharing.

## Prerequisites

- **Node.js**: Version 16 or higher (download from [nodejs.org](https://nodejs.org/)).
- **npm**: Comes with Node.js.
- **Windows**: Tested on Windows 10/11 (for portable exe builds).

## Installation

1. Clone the repository:
