use egui::Vec2b;
use egui_plot::{Legend, Line, Plot, PlotPoints};

use super::ComparisonState;
use crate::inbound::ui::app::DataCache;
use crate::inbound::ui::theme;

pub fn show(ui: &mut egui::Ui, state: &ComparisonState, cache: &DataCache) {
    let metric_name = state.metric.as_str().to_owned();

    Plot::new(ui.auto_id_with("comparison"))
        .link_axis("main_group", Vec2b::TRUE)
        .legend(Legend::default())
        .label_formatter(move |name, pt| {
            format!(
                "{}\n{}\nstep: {:.0}\nvalue: {:.6}",
                metric_name, name, pt.x, pt.y
            )
        })
        .show(ui, |plot_ui| {
            for (i, run_id) in state.run_ids.iter().enumerate() {
                let color = theme::CHART_COLORS[i % theme::CHART_COLORS.len()];

                let points: Vec<[f64; 2]> = cache
                    .get_series(run_id, &state.metric)
                    .map(|series| series.data.points().iter().map(Into::into).collect())
                    .unwrap_or_default();

                plot_ui.line(Line::new(run_id.short(), PlotPoints::new(points)).color(color));
            }
        });
}
