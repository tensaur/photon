fn main() {
    #[cfg(feature = "dashboard")]
    build_dashboard();
}

#[cfg(feature = "dashboard")]
fn build_dashboard() {
    use std::path::PathBuf;
    use std::process::Command;

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace_root = manifest_dir.join("../..");
    let dist_dir = workspace_root.join("crates/dashboard/dist");

    std::fs::create_dir_all(&dist_dir).expect("failed to create dist directory");

    let status = Command::new("cargo")
        .current_dir(&workspace_root)
        .args([
            "build",
            "-p",
            "photon-dashboard",
            "--lib",
            "--target",
            "wasm32-unknown-unknown",
            "--release",
        ])
        .status()
        .expect("failed to run cargo build for WASM target");

    if !status.success() {
        panic!("failed to compile photon-dashboard to WASM");
    }

    let wasm_file =
        workspace_root.join("target/wasm32-unknown-unknown/release/photon_dashboard.wasm");

    match Command::new("wasm-bindgen")
        .args([
            "--target",
            "web",
            "--no-typescript",
            "--out-dir",
            dist_dir.to_str().unwrap(),
            wasm_file.to_str().unwrap(),
        ])
        .status()
    {
        Ok(s) if s.success() => {}
        Ok(s) => panic!("wasm-bindgen exited with status {s}"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!(
                "cargo:warning=wasm-bindgen-cli not found. Install with: cargo install wasm-bindgen-cli"
            );
            return;
        }
        Err(e) => panic!("failed to run wasm-bindgen: {e}"),
    }

    std::fs::write(dist_dir.join("index.html"), INDEX_HTML).expect("failed to write index.html");

    println!("cargo:rerun-if-changed=../dashboard/src");
    println!("cargo:rerun-if-changed=../dashboard/Cargo.toml");
}

#[cfg(feature = "dashboard")]
const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Photon</title>
    <style>html, body, canvas { margin: 0; width: 100%; height: 100%; overflow: hidden; }</style>
</head>
<body>
    <canvas id="photon_canvas"></canvas>
    <script type="module">
        import init, { WebHandle } from './photon_dashboard.js';
        await init();
        const handle = new WebHandle();
        await handle.start(document.getElementById('photon_canvas'));
    </script>
</body>
</html>"#;
