pub mod comparison;
pub mod line_chart;

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;

pub enum Pane {
    LineChart(LineChartState),
    Comparison(ComparisonState),
}

pub struct LineChartState {
    pub run_id: RunId,
    pub metric: Metric,
}

pub struct ComparisonState {
    pub run_ids: Vec<RunId>,
    pub metric: Metric,
}
