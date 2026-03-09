@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\TEMP\testing\PS4"
set "SCRAPER=%BASE_DIR%\scraper_ps4_v2.py"

REM ── R2 / rclone config ────────────────────────────────────────────────────
REM rclone must be installed and on PATH, OR set RCLONE_EXE to full path.
REM Configure an rclone remote named "r2ps4" pointing at the ps4 bucket, OR
REM pass credentials via env vars (rclone supports RCLONE_S3_* vars).
REM
REM Minimal rclone config for Cloudflare R2:
REM   [r2ps4]
REM   type = s3
REM   provider = Cloudflare
REM   access_key_id = YOUR_KEY
REM   secret_access_key = YOUR_SECRET
REM   endpoint = https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com
REM   acl = private
REM
REM Set these if not already in rclone.conf / environment:
if not defined RCLONE_EXE set "RCLONE_EXE=rclone"
if not defined R2_REMOTE   set "R2_REMOTE=r2"

cd /d "%BASE_DIR%" || exit /b 1

REM ── Run the scraper ───────────────────────────────────────────────────────
python "%SCRAPER%"
if errorlevel 1 (
  echo [ERROR] Scraper failed. Skipping upload.
  taskkill /F /IM chromedriver.exe /T >nul 2>&1
  exit /b 1
)

REM ── Upload JSON files (always overwrite — they change every run) ──────────
echo Uploading JSON outputs...
%RCLONE_EXE% copyto games.json        "%R2_REMOTE%:ps4/games.json"        --s3-no-check-bucket
%RCLONE_EXE% copyto games_cache.json  "%R2_REMOTE%:ps4/games_cache.json"  --s3-no-check-bucket
if errorlevel 1 echo [WARN] JSON upload reported an error.

REM ── Upload screenshots (new only, fast — no full bucket scan) ─────────────
REM --no-traverse: rclone checks each local file individually against remote.
REM                Does NOT list the entire bucket first (avoids 6000+ API calls).
REM                Skips files that already exist — no re-upload of existing files.
REM The local "screenshots\" prefix is stripped automatically by rclone copy;
REM files land at ps4/{game-slug}/cover.jpg etc — matching bucket structure.
echo Uploading screenshots (new only)...
%RCLONE_EXE% copy screenshots "%R2_REMOTE%:images" --no-traverse --s3-no-check-bucket
if errorlevel 1 echo [WARN] Screenshot upload reported an error.

echo.
echo Upload complete.

REM ── Optional completion sound ─────────────────────────────────────────────
powershell -NoProfile -Command ^
  "try { (New-Object Media.SoundPlayer 'C:\Windows\Media\Windows Notify System Generic.wav').PlaySync() } catch {}" ^
  >nul 2>&1

exit /b 0
