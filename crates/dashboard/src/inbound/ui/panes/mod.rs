pub mod comparison;
pub mod line_chart;

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
