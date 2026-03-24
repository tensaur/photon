pub mod bucket;
pub mod compaction;
pub mod metric;
pub mod watermark;

pub struct ClientBuilder {
    url: String,
    database: String,
    user: String,
    password: String,
}

impl ClientBuilder {
    pub fn new() -> Self {
        Self {
            url: "http://localhost:8123".into(),
            database: "photon".into(),
            user: "default".into(),
            password: String::new(),
        }
    }

    pub fn with_env(mut self) -> Self {
        if let Ok(v) = std::env::var("CLICKHOUSE_URL") {
            self.url = v;
        }
        if let Ok(v) = std::env::var("CLICKHOUSE_DATABASE") {
            self.database = v;
        }
        if let Ok(v) = std::env::var("CLICKHOUSE_USER") {
            self.user = v;
        }
        if let Ok(v) = std::env::var("CLICKHOUSE_PASSWORD") {
            self.password = v;
        }
        self
    }

    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = database.into();
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = user.into();
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = password.into();
        self
    }

    pub fn build(self) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(&self.url)
            .with_database(&self.database)
            .with_user(&self.user)
            .with_password(&self.password)
    }
}

pub async fn migrate(client: &clickhouse::Client) -> Result<(), clickhouse::error::Error> {
    client
        .query("CREATE DATABASE IF NOT EXISTS photon")
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.metrics (
                run_id UUID,
                key String,
                step UInt64,
                value Float64,
                timestamp_ms UInt64
            ) ENGINE = MergeTree()
            PARTITION BY run_id
            ORDER BY (run_id, key, step)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.watermarks (
                run_id UUID,
                sequence UInt64
            ) ENGINE = ReplacingMergeTree(sequence)
            ORDER BY (run_id)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.buckets (
                run_id UUID,
                key String,
                tier UInt32,
                step_start UInt64,
                step_end UInt64,
                value Float64,
                min Float64,
                max Float64
            ) ENGINE = MergeTree()
            PARTITION BY run_id
            ORDER BY (run_id, key, tier, step_start)",
        )
        .execute()
        .await?;

    client
        .query(
            "CREATE TABLE IF NOT EXISTS photon.compaction_cursors (
                run_id UUID,
                key String,
                tier UInt32,
                offset UInt64
            ) ENGINE = ReplacingMergeTree(offset)
            ORDER BY (run_id, key, tier)",
        )
        .execute()
        .await?;

    Ok(())
}
