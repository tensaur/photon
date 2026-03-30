use egui::Vec2b;
use egui_plot::{Legend, Line, LineStyle, Plot, PlotPoints, VLine};

use photon_ui::theme;

use super::ComparisonState;
use crate::inbound::ui::app::DataCache;
use crate::inbound::ui::sidebar::SidebarState;

pub fn show(
    ui: &mut egui::Ui,
    state: &ComparisonState,
    cache: &DataCache,
    sidebar_state: &SidebarState,
    crosshair_x: &mut Option<f64>,
) {
    let metric_name = state.metric.as_str().to_owned();
    let crosshair_val = *crosshair_x;

    let plot_response = Plot::new(ui.auto_id_with("comparison"))
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
                let color = sidebar_state
                    .get_color(run_id)
                    .unwrap_or(theme::DARK.chart_colors[i % 8]);

                let points: Vec<[f64; 2]> = cache
                    .get_series(run_id, &state.metric)
                    .map(|series| series.data.points().iter().map(Into::into).collect())
                    .unwrap_or_default();

                plot_ui.line(
                    Line::new(run_id.short(), PlotPoints::new(points))
                        .width(2.0)
                        .color(color),
                );
            }

            if let Some(x) = crosshair_val {
                plot_ui.vline(
                    VLine::new("crosshair", x)
                        .color(egui::Color32::from_rgba_unmultiplied(255, 255, 255, 80))
                        .style(LineStyle::Dashed { length: 6.0 })
                        .width(1.0),
                );
            }
        });

    if let Some(hover_pos) = plot_response.response.hover_pos() {
        let plot_point = plot_response.transform.value_from_position(hover_pos);
        *crosshair_x = Some(plot_point.x);
    }
}
