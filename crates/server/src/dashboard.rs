#[derive(rust_embed::RustEmbed)]
#[folder = "../../crates/dashboard/dist/"]
struct Assets;

pub fn get_file(path: &str) -> Option<(Vec<u8>, String)> {
    Assets::get(path)
        .or_else(|| Assets::get("index.html"))
        .map(|file| {
            let mime = file.metadata.mimetype().to_string();
            (file.data.to_vec(), mime)
        })
}
