use axum::http::{StatusCode, Uri, header};
use axum::response::IntoResponse;
use tokio::net::TcpListener;

#[derive(rust_embed::RustEmbed)]
#[folder = "../../crates/dashboard/dist/"]
struct Assets;

pub async fn serve(listener: TcpListener) {
    let app = axum::Router::new().fallback(axum::routing::get(handle));
    axum::serve(listener, app)
        .await
        .expect("dashboard server failed");
}

async fn handle(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');
    let path = if path.is_empty() { "index.html" } else { path };

    match Assets::get(path) {
        Some(file) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, file.metadata.mimetype().to_string())],
            file.data.to_vec(),
        )
            .into_response(),
        None => match Assets::get("index.html") {
            Some(file) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "text/html".to_string())],
                file.data.to_vec(),
            )
                .into_response(),
            None => (StatusCode::NOT_FOUND, "Dashboard not built").into_response(),
        },
    }
}
