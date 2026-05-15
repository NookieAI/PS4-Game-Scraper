<div align="center">

# 🎮 PS4 Game Browser

**Browse, search, and download from a huge PS4 game library — all from one clean desktop app.**

[![Latest Release](https://img.shields.io/github/v/release/NookieAI/PS4-Game-Browser?label=latest&color=00d4ff&style=for-the-badge)](https://github.com/NookieAI/PS4-Game-Browser/releases/latest)
[![Downloads](https://img.shields.io/github/downloads/NookieAI/PS4-Game-Browser/total?color=00d4ff&style=for-the-badge)](https://github.com/NookieAI/PS4-Game-Browser/releases)
[![Platforms](https://img.shields.io/badge/platforms-Windows%20%7C%20macOS%20%7C%20Linux-00d4ff?style=for-the-badge)](#-install)

</div>

---

## ⬇️ Download

Grab the right file for your computer from the **[Latest Release](https://github.com/NookieAI/PS4-Game-Browser/releases/latest)** page:

| Your computer | Download |
|---|---|
| 🪟 **Windows 10/11** | `PS4 Game Browser v*.exe` — *portable, just double-click* |
| 🍎 **Mac (Apple Silicon — M1/M2/M3/M4)** | `PS4 Game Browser v*_aarch64.app.zip` |
| 🍎 **Mac (Intel)** | `PS4 Game Browser v*_x64.app.zip` |
| 🐧 **Linux (any)** | `PS4 Game Browser v*_amd64.AppImage` — *portable, just chmod +x* |

*Not sure which Mac you have? Click the Apple menu → About This Mac. If it says "Chip: Apple M1/M2/M3/M4", get the Apple Silicon one. If it says "Processor: Intel", get the Intel one.*

---

## ✨ What it does

- 🔍 **Instant search** across the entire library — type and results filter live
- 🆕 **Newest first** by default — see what just dropped without scrolling
- 🎨 **Cover art, descriptions, and version info** for every game
- 🔗 **Multiple download mirrors** for every release — pick the host you prefer
- 📦 **Multi-part archives** grouped cleanly per host (no more guessing which Part 03 goes with which Part 04)
- 🔄 **Auto-updates itself** — never miss a new release or library refresh
- ⚡ **Fast and quiet** — opens in under a second, sips memory, doesn't phone home

---

## 🚀 Install

### 🪟 Windows

1. Download `PS4 Game Browser v*.exe` from the [releases page](https://github.com/NookieAI/PS4-Game-Browser/releases/latest).
2. Put it anywhere you like — Desktop, Downloads, a folder on your D: drive, even a USB stick. **No installer.**
3. Double-click to run.
4. **Windows will probably show a blue "Windows protected your PC" screen** — this happens to every small app that doesn't pay Microsoft for a code-signing certificate. Click **More info**, then **Run anyway**.
5. The app remembers nothing about its location. To "uninstall," just delete the .exe. To move it, drag it anywhere.

### 🍎 Mac

1. Download the `.app.zip` file that matches your Mac (see the table above).
2. Double-click the `.zip` to unzip — you'll get **PS4 Game Browser.app**.
3. Put it anywhere you want — Applications, Desktop, an external drive. Then double-click to open.
4. The first time you open it: macOS will refuse and say *"PS4 Game Browser can't be opened because Apple cannot check it for malicious software."* This is normal for apps not sold through the App Store.
   - **Right-click** (or Control-click) the app → choose **Open** → click **Open** on the warning dialog.
   - You only have to do this once. After that, double-click works normally.
5. Future updates install automatically without re-prompting.

### 🐧 Linux

1. Download `PS4 Game Browser v*_amd64.AppImage`.
2. Right-click it → Properties → make it executable.  
   *(Or in a terminal: `chmod +x "PS4 Game Browser"*.AppImage`)*
3. Double-click to run. Put it wherever you like — it's a single file with no install step.

---

## 🔄 How updates work

Open the app. That's it.

- 8 seconds after launch, it checks for a new version.
- If one's available, you'll see a dialog: *"Update available: v4.0.1 (12 MB)? App will restart."*
- Click **OK** → it downloads in the background → app closes briefly → reopens on the new version. Takes about 30 seconds.
- Click **Cancel** if you want to skip this one. It'll ask again next time you open the app.

No accounts. No telemetry. No nagware. The update check is one quick request to GitHub and that's it.

---

## 🎯 Using the app

Once you're in:

| What you want | How to do it |
|---|---|
| 🔍 **Find a game** | Type in the search box at the top |
| 🆕 **See what's new** | The default view shows newest first |
| 🅰️ **Browse alphabetically** | Click the **Sort: Date** button — it flips to **Sort: Title** |
| 📥 **Download a game** | Click any game card → pick a mirror in the popup |
| 🔄 **Refresh the library** | Click the **Refresh** button to pull the latest list |

Multi-part downloads are grouped by host, so you'll see something like:

```
Mediafire    P01  P02  P03  P04
Vikingfile   P01  P02  P03  P04
Rootz        P01  P02  P03  P04
Gofile       Open
```

Just click each part in order and you've got the full game.

---

## ❓ Troubleshooting

<details>
<summary><b>"Windows protected your PC" on first launch</b></summary>

This is Microsoft's SmartScreen warning for apps that aren't signed with a Code Signing certificate (which costs hundreds of dollars a year for indie developers). The app is safe — the warning is just because it's new.

**Fix:** click **More info** in the blue dialog, then **Run anyway**.
</details>

<details>
<summary><b>Mac says "can't be opened" or "developer cannot be verified"</b></summary>

Standard macOS Gatekeeper behavior for apps not from the App Store. The app is safe.

**Fix:** find the app in Finder → **right-click** → **Open** → **Open** on the dialog. Only needed once.
</details>

<details>
<summary><b>App won't update / update prompt never appears</b></summary>

A few things to check:

- Make sure your computer can reach `github.com` (corporate firewalls sometimes block it).
- Close and reopen the app — the check runs 8 seconds after startup.
- If you've been opening and closing the app dozens of times within an hour, GitHub may have rate-limited your IP for an hour. Wait and try again.
- If you click **OK** on the update but nothing happens, your antivirus may be blocking the installer. Temporarily disable it and try again, or download manually from the [releases page](https://github.com/NookieAI/PS4-Game-Browser/releases/latest).
</details>

<details>
<summary><b>Two windows open when I double-click the icon</b></summary>

Shouldn't happen with v4.0.0+ — the app prevents a second copy from opening and just focuses the existing window. If you're on an older version, update.
</details>

<details>
<summary><b>Game list is empty / "Failed to load"</b></summary>

The app pulls the library list from a cloud source on launch. If that source is briefly unreachable, you'll see an empty list. Click **Refresh** after a minute. If it still doesn't load, the source may be down — check back later.
</details>

<details>
<summary><b>Multi-part downloads are confusing</b></summary>

Big games are split into multiple files. You need ALL the parts from the SAME host to extract the game.

- Pick ONE host row (e.g. Mediafire, Vikingfile, Rootz — whatever you prefer).
- Download every part in that row: P01, P02, P03, P04...
- Once they're all in the same folder, extract the first one — your archive tool (7-Zip, WinRAR, The Unarchiver) automatically pulls in the rest.

Don't mix parts from different hosts.
</details>

<details>
<summary><b>How do I uninstall?</b></summary>

Easy — there's no installer to uninstall. The app is fully portable on every platform:

- **Windows:** delete the .exe file.
- **Mac:** drag PS4 Game Browser.app to the Trash.
- **Linux:** delete the .AppImage file.

If you want to clear remembered settings too: delete the small config file the app stores in your user data folder (`%APPDATA%\com.nookie.ps4browser\` on Windows, `~/Library/Application Support/com.nookie.ps4browser/` on Mac, `~/.local/share/com.nookie.ps4browser/` on Linux).
</details>

---

## 💬 Found a bug? Have a request?

Open an [Issue](https://github.com/NookieAI/PS4-Game-Browser/issues) and describe what happened. Screenshots help a lot.

---

<div align="center">

**Made with care by [@NookieAI](https://github.com/NookieAI)** · Free to use, no strings attached.

</div>
