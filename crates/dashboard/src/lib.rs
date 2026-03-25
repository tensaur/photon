pub mod domain;
pub mod inbound;
pub mod outbound;

pub use crate::outbound::http::HttpQuerier;
pub use crate::outbound::websocket::WsSubscriber;

use crate::domain::service::Service;
use crate::inbound::ui::app::DashboardApp;

#[cfg(not(target_arch = "wasm32"))]
pub fn run(server_url: String) -> eframe::Result {
    use photon_protocol::codec::CodecKind;
    use photon_transport::codec::CodecTransport;
    use photon_transport::http::HttpTransport;
    use photon_transport::websocket::WebSocketTransport;

    let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
    let _guard = rt.enter();

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Photon Dashboard")
            .with_inner_size([1280.0, 800.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Photon Dashboard",
        options,
        Box::new(move |cc| {
            let handle = tokio::runtime::Handle::current();
            let codec = CodecKind::default();

            // Connect WebSocket for subscriptions (best-effort)
            let ws_url = server_url
                .replace("http://", "ws://")
                .replace("https://", "wss://")
                .replace("/api/query", "/api/ws");
            let ws_transport =
                handle.block_on(async { WebSocketTransport::connect(&ws_url).await.ok() });
            let ws_codec_transport = ws_transport.map(|bt| CodecTransport::new(codec.clone(), bt));

            // Build subscriber + reader transport from the same clone
            let (subscriber, reader_transport) = match ws_codec_transport {
                Some(t) => (WsSubscriber::new(t.clone()), Some(t)),
                None => {
                    let (out_tx, _out_rx) = async_channel::bounded(1);
                    let (_in_tx, in_rx) = async_channel::bounded(1);
                    let dummy = WebSocketTransport::from_channels(out_tx, in_rx);
                    let t = CodecTransport::new(codec.clone(), dummy);

                    (WsSubscriber::new(t), None)
                }
            };

            // Create querier (HTTP)
            let bt = HttpTransport::connect(&server_url);
            let transport = CodecTransport::new(codec, bt);
            let querier = HttpQuerier::new(transport);

            let service = Service::new(querier, subscriber);
            let (cmd_tx, resp_rx) =
                inbound::channel::spawn_service(cc.egui_ctx.clone(), service, reader_transport);

            Ok(Box::new(DashboardApp::new(cc, cmd_tx, resp_rx)))
        }),
    )
}

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
pub struct WebHandle {
    runner: eframe::WebRunner,
}

#[cfg(target_arch = "wasm32")]
#[wasm_bindgen]
impl WebHandle {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            runner: eframe::WebRunner::new(),
        }
    }

    #[wasm_bindgen]
    pub async fn start(&self, canvas: web_sys::HtmlCanvasElement) -> Result<(), JsValue> {
        use photon_protocol::codec::CodecKind;
        use photon_transport::codec::CodecTransport;
        use photon_transport::http::HttpTransport;
        use photon_transport::websocket::WebSocketTransport;

        let options = eframe::WebOptions::default();

        // Resolve full URLs from the browser origin (reqwest needs absolute URLs)
        let origin = web_sys::window()
            .and_then(|w| w.location().origin().ok())
            .unwrap_or_else(|| "http://localhost:50052".to_string());
        let query_url = format!("{origin}/api/query");
        let ws_url = origin
            .replace("http://", "ws://")
            .replace("https://", "wss://");
        let ws_url = format!("{ws_url}/api/ws");
        let codec = CodecKind::default();

        // Connect WebSocket for subscriptions (best-effort)
        let ws_transport = WebSocketTransport::connect(&ws_url).await.ok();
        let ws_codec_transport = ws_transport.map(|bt| CodecTransport::new(codec.clone(), bt));

        let (subscriber, reader_transport) = match ws_codec_transport {
            Some(t) => (WsSubscriber::new(t.clone()), Some(t)),
            None => {
                let (out_tx, _out_rx) = async_channel::bounded(1);
                let (_in_tx, in_rx) = async_channel::bounded(1);
                let dummy = WebSocketTransport::from_channels(out_tx, in_rx);
                let t = CodecTransport::new(codec.clone(), dummy);

                (WsSubscriber::new(t), None)
            }
        };

        self.runner
            .start(
                canvas,
                options,
                Box::new(move |cc| {
                    let bt = HttpTransport::connect(&query_url);
                    let transport = CodecTransport::new(codec, bt);
                    let querier = HttpQuerier::new(transport);
                    let service = Service::new(querier, subscriber);

                    let (cmd_tx, resp_rx) = inbound::channel::spawn_service(
                        cc.egui_ctx.clone(),
                        service,
                        reader_transport,
                    );

                    Ok(Box::new(DashboardApp::new(cc, cmd_tx, resp_rx)))
                }),
            )
            .await
    }
}
