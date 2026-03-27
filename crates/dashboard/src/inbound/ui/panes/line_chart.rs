use egui::Vec2b;
use egui_plot::{Line, Plot, PlotPoints};

use super::LineChartState;
use crate::inbound::ui::app::DataCache;

pub fn show(ui: &mut egui::Ui, state: &LineChartState, cache: &DataCache) {
    let points: Vec<[f64; 2]> = cache
        .get_series(&state.run_id, &state.metric)
        .map(|series| series.data.points().iter().map(Into::into).collect())
        .unwrap_or_default();

    let metric_name = state.metric.as_str().to_owned();

    Plot::new(ui.auto_id_with("line_chart"))
        .link_axis("main_group", Vec2b::TRUE)
        .label_formatter(move |_name, pt| {
            format!("{}\nstep: {:.0}\nvalue: {:.6}", metric_name, pt.x, pt.y)
        })
        .show(ui, |plot_ui| {
            plot_ui.line(Line::new(
                state.metric.as_str(),
                PlotPoints::new(points.clone()),
            ));
        });
}
