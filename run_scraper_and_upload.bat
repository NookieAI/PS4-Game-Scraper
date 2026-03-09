@echo off
setlocal EnableExtensions

set "BASE_DIR=C:\TEMP\testing\PS4"
set "SCRAPER=%BASE_DIR%\scraper_ps4_v2.py"
set "UPLOADER=%BASE_DIR%\upload_to_r2.py"

REM ── R2 credentials ────────────────────────────────────────────────────────
REM Set these here OR pre-set them as Windows environment variables.
REM If already set in your system environment, these lines are ignored.
if not defined R2_ACCOUNT_ID     set "R2_ACCOUNT_ID=your-account-id-here"
if not defined R2_BUCKET         set "R2_BUCKET=images"
if not defined R2_ACCESS_KEY_ID  set "R2_ACCESS_KEY_ID=your-access-key-here"
if not defined R2_SECRET_ACCESS_KEY set "R2_SECRET_ACCESS_KEY=your-secret-key-here"

cd /d "%BASE_DIR%" || exit /b 1

REM ── Run the scraper ───────────────────────────────────────────────────────
python "%SCRAPER%"
if errorlevel 1 (
  echo [ERROR] Scraper failed. Skipping upload.
  taskkill /F /IM chromedriver.exe /T >nul 2>&1
  exit /b 1
)

REM ── Upload to R2 (fast: bulk key list, skips existing files instantly) ────
REM upload_to_r2.py handles games.json, games_cache.json AND screenshots/
REM in a single run — no rclone needed.
python "%UPLOADER%"
if errorlevel 1 echo [WARN] Upload reported an error.

REM ── Optional completion sound ─────────────────────────────────────────────
powershell -NoProfile -Command ^
  "try { (New-Object Media.SoundPlayer 'C:\Windows\Media\Windows Notify System Generic.wav').PlaySync() } catch {}" ^
  >nul 2>&1

exit /b 0
