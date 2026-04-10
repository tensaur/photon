pub mod comparison;
pub mod line_chart;

use std::ops::RangeInclusive;
use std::sync::Arc;

use egui::{Color32, Mesh, Shape, Ui};
use egui_plot::{
    PlotBounds, PlotGeometry, PlotItem, PlotItemBase, PlotPoint, PlotTransform,
};

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;

pub enum Pane {
    LineChart(LineChartState),
    Comparison(ComparisonState),
}

impl Pane {
    pub fn title(&self) -> &str {
        match self {
            Pane::LineChart(s) => s.metric.as_str(),
            Pane::Comparison(s) => s.metric.as_str(),
        }
    }
}

pub struct LineChartState {
    pub run_id: RunId,
    pub metric: Metric,
}

pub struct ComparisonState {
    pub run_ids: Vec<RunId>,
    pub metric: Metric,
}

/// A min/max envelope band rendered as a single triangle-strip mesh.
///
/// Built directly as an `egui::Mesh` to avoid `egui_plot::Polygon`'s fan
/// triangulation, which only renders convex polygons correctly. Adjacent
/// `(x_i, min_i)` / `(x_i, max_i)` vertex pairs are stitched into quads,
/// so the band fills correctly regardless of how min and max wiggle and
/// has no inter-chunk anti-aliasing seams.
pub(crate) struct EnvelopeBand {
    base: PlotItemBase,
    /// `(x, min, max)` triples in increasing x order.
    samples: Vec<(f64, f64, f64)>,
    fill_color: Color32,
    bounds: PlotBounds,
}

impl EnvelopeBand {
    pub(crate) fn new(envelope: &[(f64, f64, f64)], fill_color: Color32) -> Self {
        let mut bounds = PlotBounds::NOTHING;
        for &(x, min, max) in envelope {
            bounds.extend_with(&PlotPoint::new(x, min));
            bounds.extend_with(&PlotPoint::new(x, max));
        }
        Self {
            // Empty name keeps the band out of the legend (see egui_plot Legend filter).
            base: PlotItemBase::new(String::new()),
            samples: envelope.to_vec(),
            fill_color,
            bounds,
        }
    }
}

impl PlotItem for EnvelopeBand {
    fn shapes(&self, _ui: &Ui, transform: &PlotTransform, shapes: &mut Vec<Shape>) {
        let n = self.samples.len();
        if n < 2 || self.fill_color == Color32::TRANSPARENT {
            return;
        }

        let mut mesh = Mesh::default();
        mesh.reserve_vertices(n * 2);
        mesh.reserve_triangles((n - 1) * 2);

        for &(x, min, max) in &self.samples {
            let top = transform.position_from_point(&PlotPoint::new(x, max));
            let bot = transform.position_from_point(&PlotPoint::new(x, min));
            mesh.colored_vertex(top, self.fill_color);
            mesh.colored_vertex(bot, self.fill_color);
        }

        // Two triangles per quad between adjacent (top, bot) vertex pairs.
        // Vertex layout: top_i = 2*i, bot_i = 2*i + 1.
        for i in 0..(n as u32 - 1) {
            let t0 = 2 * i;
            let b0 = 2 * i + 1;
            let t1 = 2 * (i + 1);
            let b1 = 2 * (i + 1) + 1;
            mesh.add_triangle(t0, b0, t1);
            mesh.add_triangle(b0, b1, t1);
        }

        shapes.push(Shape::Mesh(Arc::new(mesh)));
    }

    fn initialize(&mut self, _x_range: RangeInclusive<f64>) {}

    fn color(&self) -> Color32 {
        self.fill_color
    }

    fn base(&self) -> &PlotItemBase {
        &self.base
    }

    fn base_mut(&mut self) -> &mut PlotItemBase {
        &mut self.base
    }

    fn geometry(&self) -> PlotGeometry<'_> {
        // Envelope bands are decorative — no hover targets.
        PlotGeometry::None
    }

    fn bounds(&self) -> PlotBounds {
        self.bounds
    }
}
