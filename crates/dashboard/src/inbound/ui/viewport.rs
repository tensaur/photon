use egui::{Ui, WidgetText};
use egui_tiles::{TileId, Tiles, UiResponse};

use photon_ui::theme;

use super::app::DataCache;
use super::panes::{self, Pane};
use super::sidebar::SidebarState;

pub struct ViewportBehavior<'a> {
    pub cache: &'a DataCache,
    pub sidebar_state: &'a SidebarState,
    pub crosshair_x: &'a mut Option<f64>,
}

impl egui_tiles::Behavior<Pane> for ViewportBehavior<'_> {
    fn pane_ui(&mut self, ui: &mut Ui, _tile_id: TileId, pane: &mut Pane) -> UiResponse {
        // Clip to the pane's allocated rect so content doesn't overflow.
        ui.set_clip_rect(ui.max_rect());

        let dragged = photon_ui::panel_header::show(ui, pane.title());

        match pane {
            Pane::LineChart(state) => {
                panes::line_chart::show(ui, state, self.cache, self.sidebar_state, self.crosshair_x)
            }
            Pane::Comparison(state) => {
                panes::comparison::show(ui, state, self.cache, self.sidebar_state, self.crosshair_x)
            }
        }

        if dragged {
            UiResponse::DragStarted
        } else {
            UiResponse::None
        }
    }

    fn tab_title_for_pane(&mut self, pane: &Pane) -> WidgetText {
        pane.title().into()
    }

    fn is_tab_closable(&self, _tiles: &Tiles<Pane>, _tile_id: TileId) -> bool {
        false
    }

    fn gap_width(&self, _style: &egui::Style) -> f32 {
        theme::PANEL_GAP
    }

    fn simplification_options(&self) -> egui_tiles::SimplificationOptions {
        egui_tiles::SimplificationOptions {
            all_panes_must_have_tabs: false,
            ..Default::default()
        }
    }
}
