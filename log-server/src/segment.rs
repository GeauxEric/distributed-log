use crate::config::Config;
use crate::index::Index;
use crate::store::Store;
use std::io;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

struct Segment {
    index: Index,
    store: Store,
    base_offset: u64,
    next_offset: u64,
    config: Config,
}

impl Segment {
    pub fn new(dir: &Path, base_offset: u64, c: &Config) -> io::Result<Self> {
        let store_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .mode(0o644)
            .open(dir.join(format!("{}{}", base_offset, ".store")))?;
        let store = Store::new(store_file)?;

        let index_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o644)
            .open(dir.join(format!("{}{}", base_offset, ".index")))?;
        let index = Index::new(index_file, c)?;

        Ok(Segment {
            index,
            store,
            base_offset,
            next_offset: 0,
            config: Default::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SegmentConfig;
    use crate::index::ENTRY_WIDTH;
    use tempfile::tempdir;

    #[test]
    fn test_segment() {
        let dir = tempdir().unwrap();
        let config = Config {
            segment: SegmentConfig {
                max_store_bytes: 1024,
                max_index_bytes: 3 * ENTRY_WIDTH as u64,
                initial_offset: 0,
            },
        };
        Segment::new(dir.path(), 16, &config).unwrap();
    }
}
