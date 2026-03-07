# Detect the installed Chrome major version so uc downloads the matching
# ChromeDriver instead of always grabbing the latest (which may not match).
import subprocess as _subprocess
_chrome_major = None
for _chrome_bin in ("google-chrome", "google-chrome-stable", "chromium-browser", "chromium"):
    try:
        _ver_str = _subprocess.check_output(
            [_chrome_bin, "--version"], text=True, stderr=_subprocess.DEVNULL
        ).strip()
        _chrome_major = int(_ver_str.split()[2].split(".")[0])
        print(f"[browser] Detected Chrome ({_chrome_bin}) major version: {_chrome_major}")
        break
    except Exception:
        continue
if _chrome_major is None:
    print("[browser] WARNING: could not detect Chrome version — uc will auto-select ChromeDriver")
driver = uc.Chrome(options=options, version_main=_chrome_major)