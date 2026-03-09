@echo off
setlocal EnableExtensions

REM Standalone upload script — run this after the scraper to sync outputs to R2.
REM Requires rclone installed and an r2ps4 remote configured (see README).

set "BASE_DIR=C:\TEMP\testing\PS4"

if not defined RCLONE_EXE set "RCLONE_EXE=rclone"
if not defined R2_REMOTE   set "R2_REMOTE=r2ps4"

cd /d "%BASE_DIR%" || exit /b 1

echo Uploading JSON outputs...
%RCLONE_EXE% copyto games.json        "%R2_REMOTE%:ps4/games.json"        --s3-no-check-bucket
%RCLONE_EXE% copyto games_cache.json  "%R2_REMOTE%:ps4/games_cache.json"  --s3-no-check-bucket
if errorlevel 1 echo [WARN] JSON upload reported an error.

echo Uploading screenshots (new only, fast — no full bucket scan)...
%RCLONE_EXE% copy screenshots "%R2_REMOTE%:ps4" --no-traverse --s3-no-check-bucket
if errorlevel 1 echo [WARN] Screenshot upload reported an error.

echo.
echo Done.
