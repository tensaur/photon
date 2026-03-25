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
            let codec = CodecKind::default();
            let bt = HttpTransport::connect(&server_url);
            let transport = CodecTransport::new(codec, bt);
            let querier = HttpQuerier::new(transport);
            let subscriber = WsSubscriber::new();
            let service = Service::new(querier, subscriber);
            let (cmd_tx, resp_rx) = inbound::channel::spawn_service(cc.egui_ctx.clone(), service);

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

        let options = eframe::WebOptions::default();

        let origin = web_sys::window()
            .and_then(|w| w.location().origin().ok())
            .unwrap_or_else(|| "http://localhost:50052".to_string());
        let query_url = format!("{origin}/api/query");
        let codec = CodecKind::default();

        self.runner
            .start(
                canvas,
                options,
                Box::new(move |cc| {
                    let bt = HttpTransport::connect(&query_url);
                    let transport = CodecTransport::new(codec, bt);
                    let querier = HttpQuerier::new(transport);
                    let subscriber = WsSubscriber::new();
                    let service = Service::new(querier, subscriber);

                    let (cmd_tx, resp_rx) =
                        inbound::channel::spawn_service(cc.egui_ctx.clone(), service);

                    Ok(Box::new(DashboardApp::new(cc, cmd_tx, resp_rx)))
                }),
            )
            .await
    }
}
