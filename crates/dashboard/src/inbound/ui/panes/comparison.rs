use egui::Vec2b;
use egui_plot::{Corner, Legend, Line, LineStyle, Plot, PlotPoints, VLine};

use photon_core::types::query::SeriesData;
use photon_ui::envelope::Envelope;
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

    // (name, color, line_points, optional envelope as (x, min, max) triples)
    let mut all_series: Vec<(String, egui::Color32, Vec<[f64; 2]>, Option<Vec<(f64, f64, f64)>>)> =
        Vec::new();
    let mut global_x_min = f64::MAX;
    let mut global_x_max = f64::MIN;

    for (i, run_id) in state.run_ids.iter().enumerate() {
        let color = sidebar_state
            .get_color(run_id)
            .unwrap_or(theme::DARK.chart_colors[i % 8]);

        let series = cache.get_series(run_id, &state.metric);

        let (points, envelope): (Vec<[f64; 2]>, Option<Vec<(f64, f64, f64)>>) =
            match series.map(|s| &s.data) {
                Some(SeriesData::Raw { points }) => {
                    (points.iter().map(Into::into).collect(), None)
                }
                Some(SeriesData::Bucketed { buckets }) => (
                    buckets
                        .iter()
                        .map(|b| [b.step_end.as_u64() as f64, b.mean])
                        .collect(),
                    Some(
                        buckets
                            .iter()
                            .map(|b| (b.step_end.as_u64() as f64, b.min, b.max))
                            .collect(),
                    ),
                ),
                None => (Vec::new(), None),
            };

        if let Some(first) = points.first() {
            global_x_min = global_x_min.min(first[0]);
        }
        if let Some(last) = points.last() {
            global_x_max = global_x_max.max(last[0]);
        }

        let run_name = cache.run_name(run_id).unwrap_or_else(|| run_id.short());
        all_series.push((run_name, color, points, envelope));
    }

    if global_x_min > global_x_max {
        global_x_min = 0.0;
        global_x_max = 0.0;
    }

    let crosshair_val = crosshair_x.and_then(|x| {
        if x >= global_x_min && x <= global_x_max { Some(x) } else { None }
    });

    let plot_response = Plot::new(ui.auto_id_with("comparison"))
        .link_axis("main_group", Vec2b::new(true, false))
        .show_background(false)
        .show_grid(Vec2b::new(true, true))
        .legend(Legend::default().position(Corner::RightBottom).background_alpha(0.8))
        .label_formatter(move |name, pt| {
            format!(
                "{}\n{}\nstep: {:.0}\nvalue: {:.6}",
                metric_name, name, pt.x, pt.y
            )
        })
        .show(ui, |plot_ui| {
            for (name, color, points, envelope) in &all_series {
                if let Some(ref envelope) = *envelope {
                    let band_color = egui::Color32::from_rgba_unmultiplied(
                        color.r(), color.g(), color.b(), 60,
                    );
                    plot_ui.add(Envelope::new(envelope, band_color));
                }

                plot_ui.line(
                    Line::new(name.as_str(), PlotPoints::new(points.clone()))
                        .width(2.0)
                        .color(*color),
                );
            }

            if let Some(x) = crosshair_val {
                plot_ui.vline(
                    VLine::new("", x)
                        .color(egui::Color32::from_rgba_unmultiplied(255, 255, 255, 40))
                        .style(LineStyle::Dashed { length: 6.0 })
                        .width(1.0)
                        .highlight(false),
                );
            }
        });

    if let Some(hover_pos) = plot_response.response.hover_pos() {
        let plot_point = plot_response.transform.value_from_position(hover_pos);
        if plot_point.x >= global_x_min && plot_point.x <= global_x_max {
            *crosshair_x = Some(plot_point.x);
        }
    }
}
