# PS4 Game Browser — Tauri Build Instructions

## Prerequisites (one-time setup)

### 1. Install Rust
```
winget install Rustlang.Rustup
```
Or download from https://rustup.rs — click through defaults.

### 2. Install WebView2 (already installed on Windows 10/11 — skip if unsure)
https://developer.microsoft.com/en-us/microsoft-edge/webview2/

### 3. Install Visual Studio C++ Build Tools
```
winget install Microsoft.VisualStudio.2022.BuildTools
```
During install tick: "Desktop development with C++"

### 4. Install the Tauri CLI
```
cargo install tauri-cli --version "^2"
```
This project is on Tauri 2. If you have v1 CLI installed from before, run the
command above to upgrade — `cargo tauri build` will reject the project with
a schema error otherwise.

---

## Project structure

```
ps4-game-browser/
├── index.html              ← frontend (edit this for UI changes)
├── BUILD.md                ← this file
└── src-tauri/
    ├── src/
    │   └── main.rs         ← Rust backend (initializes HTTP + Shell plugins)
    ├── capabilities/
    │   └── default.json    ← v2 permission model (replaces v1 allowlist)
    ├── Cargo.toml
    ├── build.rs
    ├── tauri.conf.json     ← app config (window size, bundle, CSP)
    └── icons/              ← put your icon files here (see Icons section)
```

---

## Icons

Put your icon files in `src-tauri/icons/`. Tauri expects:

| File                      | Size      |
|---------------------------|-----------|
| `icons/32x32.png`         | 32×32     |
| `icons/128x128.png`       | 128×128   |
| `icons/128x128@2x.png`    | 256×256   |
| `icons/icon.icns`         | macOS     |
| `icons/icon.ico`          | Windows   |

**Quickest way** — generate all sizes from a single PNG using the Tauri CLI:
```
cargo tauri icon path/to/your-icon.png
```
This auto-generates every required file into `src-tauri/icons/`.

---

## Build

### Development (live preview in a window)
```
cargo tauri dev
```

### Production build
```
cargo tauri build
```

Output: `src-tauri/target/release/bundle/`
- **NSIS installer:** `nsis/PS4 Game Browser_1.0.0_x64-setup.exe`
- **Raw portable exe:** `src-tauri/target/release/ps4-game-browser.exe`

The raw `.exe` is fully portable — copy it anywhere and run it.
WebView2 must be installed on the target machine (pre-installed on Win10/11).

---

## What's baked in vs what comes from the CDN

| Thing              | Where              | How to update              |
|--------------------|--------------------|----------------------------|
| Window chrome / Rust | Inside the exe   | Rebuild                    |
| UI (index.html)    | Inside the exe     | Rebuild                    |
| `ui_patch.css`     | R2 CDN             | Upload file, instant        |
| `ui_patch.js`      | R2 CDN             | Upload file, instant        |
| `games_cache.json` | R2 CDN         | Upload file, instant        |
| Game images        | R2 CDN             | Already there from scraper  |

---

## R2 bucket layout

```
ps4/                               ← bucket root
├── games_cache.json           ← data (required)
├── ui_patch.css                   ← remote CSS patch (optional, upload empty file to disable)
├── ui_patch.js                    ← remote JS patch  (optional, upload empty file to disable)
├── CUSA04609-elden-ring-ps4/
│   ├── cover_CUSA04609.jpg
│   ├── screenshot_1_CUSA04609.jpg
│   └── screenshot_2_CUSA04609.jpg
└── ... (one folder per game, created by scraper)
```

---

## Updating the UI remotely (no rebuild needed)

Upload `ui_patch.css` or `ui_patch.js` to the R2 bucket root.
Every user gets the change on their next app launch.

See the earlier documentation for the full `UIHooks` API available in `ui_patch.js`.
