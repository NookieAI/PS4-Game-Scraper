// build.rs — Tauri build hook with version-drift guard.
//
// Verifies that the `version` field in Cargo.toml matches the `version` field
// in tauri.conf.json before invoking tauri_build::build(). If they diverge,
// the build fails fast with a clear message — saves you from shipping a
// binary whose auto-updater identity (CARGO_PKG_VERSION) disagrees with its
// bundle metadata (tauri.conf.json version).

fn main() {
    // Recompile when the token-gated catalog secrets change so option_env! in
    // main.rs picks up new values — env!/option_env! do NOT trigger a rebuild on
    // their own, so without this, rotating CATALOG_TOKEN would reuse a stale build.
    println!("cargo:rerun-if-env-changed=CATALOG_URL");
    println!("cargo:rerun-if-env-changed=CATALOG_TOKEN");
    verify_version_consistency();
    tauri_build::build()
}

fn verify_version_consistency() {
    let cargo_version = env!("CARGO_PKG_VERSION");

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR not set — are you running this outside cargo?");
    let conf_path = format!("{manifest_dir}/tauri.conf.json");

    // Re-run this check whenever tauri.conf.json changes. Cargo.toml is
    // already tracked by cargo itself.
    println!("cargo:rerun-if-changed=tauri.conf.json");

    let conf = std::fs::read_to_string(&conf_path)
        .unwrap_or_else(|e| panic!("failed to read {conf_path}: {e}"));

    // Find the first `"version": "X.Y.Z"` line. tauri.conf.json's top-level
    // version is the only "version" key at depth-1 and appears near the top,
    // so first-match is safe.
    let conf_version = conf
        .lines()
        .find_map(|line| {
            let l = line.trim_start();
            if l.starts_with("\"version\"") {
                // Split on `"` to extract value:
                //   "version": "4.0.0",
                //   ↑0       ↑1↑2  ↑3
                l.split('"').nth(3).map(|v| v.to_string())
            } else {
                None
            }
        })
        .expect("no \"version\" field found in tauri.conf.json");

    if cargo_version != conf_version {
        panic!(
            "\n\n\
             ╔════════════════════════════════════════════════════════════════╗\n\
             ║  VERSION MISMATCH between Cargo.toml and tauri.conf.json        ║\n\
             ║                                                                ║\n\
             ║    Cargo.toml       version = {cargo:<32} ║\n\
             ║    tauri.conf.json  version = {conf:<32} ║\n\
             ║                                                                ║\n\
             ║  Update BOTH files to the same version before building so the  ║\n\
             ║  bundle metadata and the auto-updater identity agree.          ║\n\
             ╚════════════════════════════════════════════════════════════════╝\n",
            cargo = cargo_version,
            conf  = conf_version,
        );
    }
}
