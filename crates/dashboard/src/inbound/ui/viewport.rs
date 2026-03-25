use egui::{Color32, Ui, WidgetText};
use egui_tiles::{TileId, Tiles, UiResponse};

use super::app::DataCache;
use super::panes::{self, Pane};
use super::theme;

pub struct ViewportBehavior<'a> {
    pub cache: &'a DataCache,
}

impl<'a> egui_tiles::Behavior<Pane> for ViewportBehavior<'a> {
    fn pane_ui(&mut self, ui: &mut Ui, _tile_id: TileId, pane: &mut Pane) -> UiResponse {
        match pane {
            Pane::LineChart(state) => panes::line_chart::show(ui, state, self.cache),
            Pane::Comparison(state) => panes::comparison::show(ui, state, self.cache),
        }
        UiResponse::None
    }

    fn tab_title_for_pane(&mut self, pane: &Pane) -> WidgetText {
        match pane {
            Pane::LineChart(state) => state.metric.as_str().into(),
            Pane::Comparison(state) => format!("Compare: {}", state.metric.as_str()).into(),
        }
    }

    fn is_tab_closable(&self, _tiles: &Tiles<Pane>, _tile_id: TileId) -> bool {
        true
    }

    fn tab_bar_color(&self, _visuals: &egui::Visuals) -> Color32 {
        theme::BG_SIDEBAR
    }

    fn tab_bg_color(
        &self,
        _visuals: &egui::Visuals,
        _tiles: &Tiles<Pane>,
        _tile_id: TileId,
        active: &egui_tiles::TabState,
    ) -> Color32 {
        if active.active {
            theme::BG_PRIMARY
        } else {
            theme::BG_SURFACE
        }
    }

    fn gap_width(&self, _style: &egui::Style) -> f32 {
        2.0
    }

    fn simplification_options(&self) -> egui_tiles::SimplificationOptions {
        egui_tiles::SimplificationOptions {
            all_panes_must_have_tabs: true,
            ..Default::default()
        }
    }
}
