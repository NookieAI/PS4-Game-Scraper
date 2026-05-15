// PS4 Game Browser — Tauri 2 backend (production)
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::Command;
use tauri::Manager;   // for app.get_webview_window() in single-instance callback

const GH_OWNER: &str = "NookieAI";
const GH_REPO:  &str = "PS4-Game-Scraper";
// ── GitHub API types ─────────────────────────────────────────────────────────
// Only the fields we actually use are deserialized — serde silently ignores
// every other field in the JSON response (release name, prerelease flag,
// author, etc). The `/releases/latest` endpoint already excludes prereleases
// server-side, so we don't need a local bool check.
#[derive(Deserialize)]
struct GithubRelease {
    tag_name: String,
    body:     Option<String>,
    assets:   Vec<GithubAsset>,
    html_url: String,
}

#[derive(Deserialize)]
struct GithubAsset {
    name:                 String,
    browser_download_url: String,
    size:                 u64,
}

// ── Return type to the frontend ──────────────────────────────────────────────
#[derive(Serialize)]
struct UpdateInfo {
    available:    bool,
    current:      String,
    latest:       Option<String>,
    download_url: Option<String>,
    asset_name:   Option<String>,
    asset_size:   Option<u64>,
    release_url:  Option<String>,
    notes:        Option<String>,
    /// Filled when no compatible installer/exe found on the latest release,
    /// even though the version is newer. Frontend can show a link to the
    /// release page so user can install manually.
    error:        Option<String>,
}

// ── Public commands ──────────────────────────────────────────────────────────
#[tauri::command]
async fn check_for_updates() -> Result<UpdateInfo, String> {
    let current = env!("CARGO_PKG_VERSION").to_string();
    let release = fetch_latest_release().await
        .map_err(|e| format!("Update check failed: {e}"))?;

    // Destructure so we can move fields out without cloning. Each return
    // branch below consumes `release_url` and `notes` exactly once.
    let GithubRelease { tag_name, body, assets, html_url } = release;
    let latest      = tag_name.trim_start_matches('v').to_string();
    let release_url = Some(html_url);
    let notes       = body;

    if !is_newer(&latest, &current) {
        return Ok(UpdateInfo {
            available: false, current, latest: Some(latest.clone()),
            download_url: None, asset_name: None, asset_size: None,
            release_url, notes, error: None,
        });
    }

    match find_asset_for_platform(&assets) {
        Some(asset) => Ok(UpdateInfo {
            available:    true,
            current,
            latest:       Some(latest.clone()),
            download_url: Some(asset.browser_download_url.clone()),
            asset_name:   Some(asset.name.clone()),
            asset_size:   Some(asset.size),
            release_url,
            notes,
            error:        None,
        }),
        None => {
            let err_msg = format!(
                "v{latest} is available but no installer for this platform was found on the release."
            );
            Ok(UpdateInfo {
                available:    true,
                current,
                latest:       Some(latest.clone()),
                download_url: None,
                asset_name:   None,
                asset_size:   None,
                release_url,
                notes,
                error:        Some(err_msg),
            })
        }
    }
}

#[tauri::command]
async fn install_update(download_url: String,
                        asset_name:   Option<String>) -> Result<String, String> {
    // Download the installer to a temp file. 5min timeout covers slow
    // connections without freezing forever on a stalled download.
    let client = reqwest::Client::builder()
        .user_agent("PS4GameBrowser-Updater/1.0")
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| format!("client init: {e}"))?;

    let bytes = client.get(&download_url)
        .send()
        .await
        .map_err(|e| format!("download request: {e}"))?
        .error_for_status()
        .map_err(|e| format!("download status: {e}"))?
        .bytes()
        .await
        .map_err(|e| format!("download body: {e}"))?;

    // Prefer the API-supplied filename (preserves spaces in "PS4 Game Browser.exe"
    // etc). Fall back to URL basename with %20 decoded if not provided.
    let safe_name = asset_name.unwrap_or_else(|| {
        download_url.rsplit('/')
            .next()
            .unwrap_or("ps4-game-browser-update.bin")
            .replace("%20", " ")
    });

    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join(&safe_name);
    std::fs::write(&temp_file, &bytes)
        .map_err(|e| format!("write temp: {e}"))?;

    let current_exe = std::env::current_exe()
        .map_err(|e| format!("current_exe: {e}"))?;
    let pid = std::process::id();

    spawn_updater(&temp_file, &current_exe, pid)
        .map_err(|e| format!("spawn updater: {e}"))?;

    Ok("Update downloaded — app will restart momentarily.".to_string())
}

// ── Internals ────────────────────────────────────────────────────────────────
async fn fetch_latest_release() -> Result<GithubRelease, String> {
    let url = format!(
        "https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/releases/latest"
    );
    // 10s timeout for the API check — if GitHub is down or rate-limiting,
    // fail fast rather than blocking app startup behind a hung TCP connection.
    let client = reqwest::Client::builder()
        .user_agent("PS4GameBrowser-Updater/1.0")
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client.get(&url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json::<GithubRelease>().await.map_err(|e| e.to_string())
}

/// Strict semver-ish comparison: split on '.' and compare numeric segments.
/// "1.2.3" > "1.2.0"  ;  "1.2.0" > "1.1.5" ;  "1.2" == "1.2.0".
/// Non-numeric junk in either side compares as 0 for that segment.
fn is_newer(latest: &str, current: &str) -> bool {
    fn parts(s: &str) -> Vec<u32> {
        s.split(|c: char| c == '.' || c == '-')
            .filter_map(|p| p.parse().ok())
            .collect()
    }
    let l = parts(latest);
    let c = parts(current);
    for i in 0..l.len().max(c.len()) {
        let lv = l.get(i).copied().unwrap_or(0);
        let cv = c.get(i).copied().unwrap_or(0);
        if lv > cv { return true; }
        if lv < cv { return false; }
    }
    false
}

/// Pick the right asset to download for the running platform.
fn find_asset_for_platform(assets: &[GithubAsset]) -> Option<&GithubAsset> {
    #[cfg(target_os = "windows")]
    {
        // Any .exe on the release — with the custom workflow naming
        // ("PS4 Game Browser v4.0.0.exe") there's exactly one Windows asset.
        return assets.iter().find(|a| a.name.to_lowercase().ends_with(".exe"));
    }
    #[cfg(target_os = "macos")]
    {
        // .app inside .zip — fully portable, no install step.
        // std::env::consts::ARCH is "aarch64" on Apple Silicon, "x86_64" on Intel.
        let arch = std::env::consts::ARCH;
        if arch == "aarch64" {
            if let Some(a) = assets.iter().find(|a|
                a.name.to_lowercase().contains("aarch64")
                && a.name.to_lowercase().ends_with(".app.zip")
            ) { return Some(a); }
        }
        return assets.iter().find(|a|
            a.name.to_lowercase().ends_with(".app.zip")
            && !a.name.to_lowercase().contains("aarch64")
        );
    }
    #[cfg(target_os = "linux")]
    {
        // AppImage only — self-contained portable, no system install.
        return assets.iter().find(|a|
            a.name.to_lowercase().ends_with(".appimage")
        );
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    return None;
}

#[cfg(target_os = "windows")]
fn spawn_updater(downloaded: &PathBuf, current_exe: &PathBuf, pid: u32)
    -> Result<(), std::io::Error>
{
    use std::os::windows::process::CommandExt;
    const DETACHED_PROCESS:          u32 = 0x00000008;
    const CREATE_NEW_PROCESS_GROUP:  u32 = 0x00000200;
    const CREATE_NO_WINDOW:          u32 = 0x08000000;

    let script_path = std::env::temp_dir()
        .join("ps4-game-browser-update.bat");

    // Portable file-swap: download is the raw .exe, not an installer.
    // We delete the old exe and rename the downloaded one in its place.
    // Wait-for-parent-exit loop is capped at ~30s to avoid infinite spin
    // if the OS leaves a zombie process.
    let script = format!(
        r#"@echo off
set /a wait_count=0
:waitloop
tasklist /FI "PID eq {pid}" 2>NUL | findstr /I /C:"{pid}" >NUL
if errorlevel 1 goto proceed
set /a wait_count+=1
if %wait_count% GEQ 30 goto proceed
timeout /t 1 /nobreak >nul
goto waitloop
:proceed
timeout /t 2 /nobreak >nul
del /F /Q "{current_exe}"
move /Y "{downloaded}" "{current_exe}"
timeout /t 1 /nobreak >nul
start "" "{current_exe}"
del "%~f0"
"#,
        pid          = pid,
        downloaded   = downloaded.display(),
        current_exe  = current_exe.display(),
    );
    std::fs::write(&script_path, script)?;

    let script_str = script_path.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "updater script path is not valid UTF-8",
        )
    })?;

    Command::new("cmd")
        .args(["/C", script_str])
        .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW)
        .spawn()?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn spawn_updater(downloaded: &PathBuf, current_exe: &PathBuf, pid: u32)
    -> Result<(), std::io::Error>
{
    use std::os::unix::fs::PermissionsExt;
    let script_path = std::env::temp_dir().join("ps4-game-browser-update.sh");

    let is_deb = downloaded
        .extension()
        .map(|e| e == "deb")
        .unwrap_or(false);

    let script = if is_deb {
        // .deb extraction WITHOUT dpkg-deb. `ar` (binutils) is universally
        // available across distros — Fedora, Arch, openSUSE, Alpine, etc. all
        // ship binutils. We unpack debian's `ar` archive to a tempdir, then
        // expand data.tar.{gz,xz,zst} with progressive fallbacks.
        format!(
            r#"#!/bin/bash
set -e
while kill -0 {pid} 2>/dev/null; do sleep 0.5; done
sleep 1

EXTRACT_DIR="$(mktemp -d)"
cd "$EXTRACT_DIR"

# A .deb is an `ar` archive containing debian-binary, control.tar.*, data.tar.*.
ar x "{downloaded}"

# Locate the data tarball — compression varies between Tauri versions.
DATA_TAR="$(ls data.tar.* 2>/dev/null | head -1)"
if [ -z "$DATA_TAR" ]; then
    echo "Update failed: no data.tar.* in .deb"
    rm -rf "$EXTRACT_DIR" "{downloaded}"
    rm -f "$0"
    exit 1
fi

# Extract — tar auto-detects most compressions; zst needs explicit handling
# on older tar. Try modern tar first, fall back to unzstd if available.
case "$DATA_TAR" in
    *.zst)
        if tar --zstd -xf "$DATA_TAR" 2>/dev/null; then
            true
        elif command -v unzstd >/dev/null 2>&1; then
            unzstd -c "$DATA_TAR" | tar -xf -
        else
            echo "Update failed: tar lacks zstd support and unzstd not found"
            rm -rf "$EXTRACT_DIR" "{downloaded}"
            rm -f "$0"
            exit 1
        fi
        ;;
    *)
        # gz, xz, bz2, etc. — tar handles transparently with -xf
        tar -xf "$DATA_TAR"
        ;;
esac

# Locate the binary inside the extracted .deb tree. Three-tier strategy:
#   1. Tauri's standard install path (current): usr/bin/ps4-game-browser
#   2. Any ELF executable whose name contains ps4/game/browser fragments
#      (covers future Tauri layout changes like opt/PS4 Game Browser/)
#   3. Any ELF executable, last resort
NEW_BIN="$(find . -type f -name 'ps4-game-browser' | head -1)"

if [ -z "$NEW_BIN" ] && command -v file >/dev/null 2>&1; then
    while IFS= read -r candidate; do
        if file -b "$candidate" 2>/dev/null | grep -q 'ELF.*executable'; then
            case "$(basename "$candidate" | tr '[:upper:]' '[:lower:]')" in
                *ps4*|*browser*|*game*)
                    NEW_BIN="$candidate"
                    break
                    ;;
            esac
        fi
    done < <(find . -type f 2>/dev/null)
fi

if [ -z "$NEW_BIN" ] && command -v file >/dev/null 2>&1; then
    while IFS= read -r candidate; do
        if file -b "$candidate" 2>/dev/null | grep -q 'ELF.*executable'; then
            NEW_BIN="$candidate"
            break
        fi
    done < <(find . -type f 2>/dev/null)
fi

if [ -z "$NEW_BIN" ]; then
    echo "Update failed: no executable binary found in .deb"
    rm -rf "$EXTRACT_DIR" "{downloaded}"
    rm -f "$0"
    exit 1
fi

cp -f "$NEW_BIN" "{current_exe}"
chmod +x "{current_exe}"
rm -rf "$EXTRACT_DIR" "{downloaded}"
nohup "{current_exe}" >/dev/null 2>&1 &
rm -f "$0"
"#,
            pid = pid,
            downloaded = downloaded.display(),
            current_exe = current_exe.display(),
        )
    } else {
        // AppImage path — self-contained, just swap the file in place
        format!(
            r#"#!/bin/bash
set -e
while kill -0 {pid} 2>/dev/null; do sleep 0.5; done
sleep 1
chmod +x "{downloaded}"
mv -f "{downloaded}" "{current_exe}"
chmod +x "{current_exe}"
nohup "{current_exe}" >/dev/null 2>&1 &
rm -f "$0"
"#,
            pid = pid,
            downloaded = downloaded.display(),
            current_exe = current_exe.display(),
        )
    };
    std::fs::write(&script_path, script)?;
    std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))?;
    Command::new("bash").arg(&script_path).spawn()?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn spawn_updater(downloaded: &PathBuf, current_exe: &PathBuf, pid: u32)
    -> Result<(), std::io::Error>
{
    use std::os::unix::fs::PermissionsExt;

    // current_exe points to .../MyApp.app/Contents/MacOS/binary — walk up
    // ancestors to find the .app bundle root we need to replace.
    let app_bundle = current_exe
        .ancestors()
        .find(|p| p.extension().map(|e| e == "app").unwrap_or(false))
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| {
            current_exe.parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.parent())
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| current_exe.clone())
        });

    let script_path = std::env::temp_dir().join("ps4-game-browser-update.sh");
    let unzip_dir   = std::env::temp_dir().join("ps4-browser-update-extract");

    // Flow (portable .app.zip variant):
    //   1. Wait for parent to exit.
    //   2. Extract the .app.zip with `ditto -xk` (Apple-aware unzip — preserves
    //      extended attributes, code signatures if present, resource forks).
    //   3. Find the .app inside the extracted directory.
    //   4. Attempt `ditto` direct replace of the current .app (no elevation).
    //   5. If direct replace fails (e.g. /Applications under parental controls,
    //      MDM-managed Mac, or a Gatekeeper-locked path), retry via osascript
    //      with administrator privileges (native Touch ID / password prompt).
    //   6. Strip Gatekeeper quarantine xattr so relaunch doesn't re-prompt.
    //   7. Relaunch + clean up.
    let script = format!(
        r#"#!/bin/bash
set -e
while kill -0 {pid} 2>/dev/null; do sleep 0.5; done
sleep 1

# Defensive cleanup: previous interrupted update may have left a stale extract
# directory. Remove and recreate before extracting.
rm -rf "{unzip}"
mkdir -p "{unzip}"

# ditto -xk: extract a .zip preserving HFS+ metadata. Safer than `unzip` for
# .app bundles since it keeps quarantine xattrs, code signatures, etc.
ditto -xk "{downloaded}" "{unzip}"

NEW_APP="$(find "{unzip}" -maxdepth 3 -name '*.app' -type d | head -1)"
if [ -z "$NEW_APP" ]; then
    rm -rf "{unzip}" "{downloaded}"
    osascript -e 'display alert "Update failed: no .app found inside the downloaded zip."' >/dev/null 2>&1 || true
    rm -f "$0"
    exit 1
fi

# Try direct replace first (no password prompt). Works for: app in
# ~/Applications, or /Applications when the user owns the dir.
DIRECT_OK=0
if rm -rf "{app_bundle}" 2>/dev/null && \
   ditto "$NEW_APP" "{app_bundle}" 2>/dev/null; then
    DIRECT_OK=1
fi

if [ "$DIRECT_OK" -eq 0 ]; then
    # Elevation path: native Touch ID / password dialog naming our app.
    ELEVATED_SCRIPT="rm -rf '{app_bundle}' && ditto '$NEW_APP' '{app_bundle}' && xattr -dr com.apple.quarantine '{app_bundle}'"
    if ! osascript -e "do shell script \"$ELEVATED_SCRIPT\" with administrator privileges with prompt \"PS4 Game Browser needs permission to install the update.\"" >/dev/null 2>&1; then
        rm -rf "{unzip}" "{downloaded}"
        osascript -e 'display alert "Update was cancelled or could not complete. The previous version is still installed."' >/dev/null 2>&1 || true
        rm -f "$0"
        exit 1
    fi
else
    xattr -dr com.apple.quarantine "{app_bundle}" 2>/dev/null || true
fi

rm -rf "{unzip}" "{downloaded}"

open "{app_bundle}"
rm -f "$0"
"#,
        pid        = pid,
        downloaded = downloaded.display(),
        unzip      = unzip_dir.display(),
        app_bundle = app_bundle.display(),
    );
    std::fs::write(&script_path, script)?;
    std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))?;
    Command::new("bash").arg(&script_path).spawn()?;
    Ok(())
}

// ── Tauri entrypoint ─────────────────────────────────────────────────────────
fn main() {
    tauri::Builder::default()
        // single-instance MUST be registered first. When a second copy of the
        // exe launches, this callback fires inside the FIRST instance with
        // the second one's argv + cwd, and the second process exits cleanly.
        // We use the AppHandle to find the main window, un-minimize it, and
        // bring it to the front so the user sees the existing app.
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.unminimize();
                let _ = window.show();
                let _ = window.set_focus();
            }
        }))
        .plugin(tauri_plugin_http::init())
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![
            check_for_updates,
            install_update,
        ])
        .run(tauri::generate_context!())
        .expect("error while running PS4 Game Browser");
}
