# Auto-Updater (no signing required)

The app checks `https://api.github.com/repos/NookieAI/PS4-Game-Scraper/releases/latest` 8 seconds after launch. If a newer version is found, the user gets a confirmation dialog, accepts, and the app downloads + installs + relaunches.

**No keypair. No signing. No setup.** Just push a tag and the workflow builds + uploads bundles. Running apps pick up the new release on next launch.

## How it works

1. JS calls Rust command `check_for_updates` → Rust hits the GitHub releases API.
2. Rust compares `tag_name` (stripped of `v` prefix) to compile-time `CARGO_PKG_VERSION`.
3. If newer, Rust finds the right asset for the running platform (Windows: `*setup.exe`, macOS: `*.dmg`, Linux: `*.AppImage`).
4. JS shows confirm dialog with version + size: *"Update to v1.0.1 (12.3 MB)? App will restart."*
5. On accept, JS calls `install_update` with the asset URL.
6. Rust downloads the installer to `%TEMP%/`, writes a platform-specific updater script, spawns it detached.
7. Current app process exits (JS calls `window.close()`).
8. Updater script waits for the PID to die, then:
   - **Windows:** runs NSIS installer with `/S` (silent), waits 3s, relaunches the installed exe.
   - **macOS:** mounts the `.dmg` via `hdiutil`, replaces the running `.app` bundle with `ditto`, strips the `com.apple.quarantine` xattr so Gatekeeper doesn't re-prompt, detaches the DMG, opens the new `.app`. No user interaction needed.
   - **Linux (AppImage):** chmods +x the new AppImage, moves it over the current path, relaunches.
   - **Linux (.deb):** extracts the .deb with `dpkg-deb -x` to a temp dir, copies the binary over the current exe, relaunches. Avoids needing sudo.

## To release a new version

```bash
# bump version in Cargo.toml + tauri.conf.json (both must match)
git commit -am "bump v4.0.1"
git tag v4.0.1
git push origin main --tags
```

The workflow:
1. Creates a draft GitHub release for the tag (or reuses existing).
2. Builds Windows/macOS-Intel/macOS-ARM/Linux in parallel.
3. Uploads each platform's installer + bundle to the release.
4. Publishes the draft as the latest release if any uploads succeeded.

Running apps will pick this up automatically on their next launch.

## Limitations

- **Windows: requires `installMode: currentUser` install path.** If a user installed your app to `C:\Program Files\` manually (not via the NSIS installer), the silent install will fail with permissions. Configured correctly out-of-the-box — only fails for users who customized their install location.
- **macOS: unsigned-app first-launch warning is gone after first install but DOES appear on the original install** ("PS4 Game Browser can't be opened because Apple cannot check it for malicious software" — user right-clicks → Open). After they trust it once, the auto-updater strips the quarantine xattr from replacement bundles so the warning doesn't recur. True code-signing ($99/yr Apple Developer Program) eliminates the first-launch prompt entirely.
- **Pre-release tags ignored:** the GitHub API endpoint `/releases/latest` only returns published non-prerelease releases. To roll out gradually, mark releases as `prerelease: true` until you're ready.
- **No rollback if updated version crashes on launch.** User has to manually download an older release. Could be mitigated by keeping a backup of the old exe before replacing, but not implemented.
- **No update signature verification.** Anyone who can intercept GitHub's HTTPS connection AND has a valid github.com cert could theoretically push a malicious update. This is the trade-off vs the keypair-signed approach. In practice it requires breaking TLS or compromising GitHub itself — not a realistic threat for most apps. If you do need signature verification, switch to `tauri-plugin-updater` (see `SETUP_AUTOUPDATER.md` history in git, or generate a new one).

## Troubleshooting

**"No installer found for this platform":** GitHub release lacks a bundle for the running OS. Check the Actions log — if a platform's build failed, it won't upload that asset. fail-fast is off so partial-platform releases happen and the updater handles them by showing a "manual install" prompt with the release page link.

**Update prompt never appears:** open the app via PowerShell with `--debug` to see Rust errors, OR run `cargo tauri dev` locally to see the console. Most common cause: GitHub API rate-limit (60/hour for unauthed requests). Each app launch makes one API call.

**Update downloads but doesn't install:** Check `%TEMP%\ps4-game-browser-update.bat` exists after the prompt. Run it manually to see error output. Most common: a security app (AV) is blocking the spawned cmd.

**Want to test without cutting a real release:** temporarily change the Rust constants `GH_OWNER`/`GH_REPO` in `main.rs` to a test repo, push a fake release there, observe behavior.
