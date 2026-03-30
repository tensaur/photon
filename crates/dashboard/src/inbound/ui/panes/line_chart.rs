use egui::Vec2b;
use egui_plot::{Line, LineStyle, Plot, PlotPoints, VLine};

use photon_ui::theme;

use super::LineChartState;
use crate::inbound::ui::app::DataCache;
use crate::inbound::ui::sidebar::SidebarState;

pub fn show(
    ui: &mut egui::Ui,
    state: &LineChartState,
    cache: &DataCache,
    sidebar_state: &SidebarState,
    crosshair_x: &mut Option<f64>,
) {
    let points: Vec<[f64; 2]> = cache
        .get_series(&state.run_id, &state.metric)
        .map(|series| series.data.points().iter().map(Into::into).collect())
        .unwrap_or_default();

    // Compute data x range for clamping crosshair.
    let x_min = points.first().map_or(0.0, |p| p[0]);
    let x_max = points.last().map_or(0.0, |p| p[0]);

    let metric_name = state.metric.as_str().to_owned();
    let color = sidebar_state
        .get_color(&state.run_id)
        .unwrap_or(theme::DARK.chart_colors[0]);

    // Clamp crosshair to data range so VLine doesn't expand plot bounds.
    let crosshair_val = crosshair_x.and_then(|x| {
        if x >= x_min && x <= x_max { Some(x) } else { None }
    });

    let plot_response = Plot::new(ui.auto_id_with("line_chart"))
        .link_axis("main_group", Vec2b::new(true, false))
        .label_formatter(move |_name, pt| {
            format!("{}\nstep: {:.0}\nvalue: {:.6}", metric_name, pt.x, pt.y)
        })
        .show(ui, |plot_ui| {
            plot_ui.line(
                Line::new(state.metric.as_str(), PlotPoints::new(points.clone()))
                    .width(2.0)
                    .color(color),
            );

            if let Some(x) = crosshair_val {
                plot_ui.vline(
                    VLine::new("", x)
                        .color(egui::Color32::from_rgba_unmultiplied(255, 255, 255, 80))
                        .style(LineStyle::Dashed { length: 6.0 })
                        .width(1.0)
                        .highlight(false),
                );
            }
        });

    // Only update crosshair if pointer is within data range.
    if let Some(hover_pos) = plot_response.response.hover_pos() {
        let plot_point = plot_response.transform.value_from_position(hover_pos);
        if plot_point.x >= x_min && plot_point.x <= x_max {
            *crosshair_x = Some(plot_point.x);
        }
    }
}
