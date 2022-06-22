use crate::config::Config;
use crate::index::Index;
use crate::store::Store;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use prost::Message;
use protos::log::v1::Record;
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
        let next_offset = {
            if index.is_empty() {
                base_offset
            } else {
                let (off, _) = index.read(-1)?;
                base_offset + (off as u64) + 1
            }
        };
        Ok(Segment {
            index,
            store,
            base_offset,
            next_offset,
            config: c.clone(),
        })
    }

    pub fn append(&mut self, record: &mut Record) -> Result<u64> {
        let mut b = BytesMut::new();
        record.encode(&mut b)?;
        let cur = self.next_offset;
        record.offset = cur;
        let (_, pos) = self.store.append(&b)?;
        self.index.write((cur - self.base_offset) as u32, pos)?;
        self.next_offset += 1;
        Ok(cur)
    }

    pub fn read(&mut self, offset: u64) -> Result<Record> {
        let (_, pos) = self.index.read((offset - self.base_offset) as i64)?;
        let payload = self.store.read(pos)?;
        let b: Bytes = payload.into();
        let r = Record::decode(b)?;
        Ok(r)
    }

    pub fn is_maxed(&self) -> bool {
        self.store.size() >= self.config.segment.max_store_bytes
            || self.index.size() >= self.config.segment.max_index_bytes
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
        let mut segment = Segment::new(dir.path(), 16, &config).unwrap();
        assert_eq!(16, segment.next_offset);
        let mut r1 = Record {
            value: vec![1, 2, 3],
            ..Default::default()
        };
        segment.append(&mut r1).unwrap();
        assert_eq!(16, r1.offset);
        let g1 = segment.read(16).unwrap();
        assert_eq!(g1.value, r1.value);
        segment.append(&mut r1).unwrap();
        assert_eq!(17, r1.offset);
        segment.append(&mut r1).unwrap();
        assert_eq!(18, r1.offset);

        let config = Config {
            segment: SegmentConfig {
                max_store_bytes: r1.value.len() as u64 * 3, // store file is maxed out
                max_index_bytes: 1024,
                initial_offset: 0,
            },
        };
        let segment = Segment::new(dir.path(), 16, &config).unwrap();
        assert!(segment.is_maxed());
    }
}
