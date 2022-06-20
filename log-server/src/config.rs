#[derive(Default)]
pub(crate) struct SegmentConfig {
    pub max_store_bytes: u64,
    pub max_index_bytes: u64,
    pub initial_offset: u64,
}

#[derive(Default)]
pub(crate) struct Config {
    pub segment: SegmentConfig,
}
