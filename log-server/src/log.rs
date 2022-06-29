use crate::config::Config;
use crate::segment::Segment;
use anyhow::{anyhow, Result};
use protos::log::v1::Record;
use std::collections::HashSet;
use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::sync;

struct Log {
    lock: sync::RwLock<()>,
    dir: PathBuf,
    config: Config,
    segments: Vec<Segment>,
    active_segment_idx: Option<usize>,
}

impl Log {
    fn new(dir: &Path, config: Config) -> Result<Log> {
        if !dir.is_dir() {
            return Err(anyhow!("{:?} is not a directory", dir));
        }
        let mut config = config;
        if config.segment.max_store_bytes == 0 {
            config.segment.max_store_bytes = 1024;
        }
        if config.segment.max_index_bytes == 0 {
            config.segment.max_index_bytes = 1024;
        }
        let mut log = Log {
            lock: sync::RwLock::new(()),
            dir: dir.into(),
            config,
            segments: vec![],
            active_segment_idx: None,
        };
        log.setup()?;
        Ok(log)
    }

    fn new_segment(&mut self, off: u64) -> Result<()> {
        let _m = self.lock.write().unwrap();
        let s = Segment::new(&self.dir, off, &self.config)?;
        self.segments.push(s);
        self.active_segment_idx = Some(self.segments.len() - 1);
        Ok(())
    }

    fn append(&mut self, record: &mut Record) -> Result<u64> {
        let _l = self.lock.write().unwrap();
        if self.active_segment_idx.is_none() {
            return Err(anyhow!("there is not active segment"));
        }
        let s = self
            .segments
            .get_mut(self.active_segment_idx.unwrap())
            .unwrap();
        let offset = s.append(record)?;
        drop(_l);
        if s.is_maxed() {
            self.new_segment(offset + 1)?;
        }
        Ok(offset)
    }

    fn setup(&mut self) -> Result<()> {
        let paths = read_dir(&self.dir)?;
        let files: Vec<PathBuf> = paths
            .filter(|entry| entry.is_ok())
            .map(|entry| entry.unwrap().path())
            .filter(|p| p.is_file())
            .collect();
        let mut base_offsets = HashSet::new();
        for path in &files {
            let off_str = path.file_stem().unwrap();
            let off = off_str.to_str().unwrap().parse::<u64>().unwrap();
            base_offsets.insert(off);
        }
        let mut base_offsets = Vec::from_iter(base_offsets);
        base_offsets.sort_unstable();
        for base_offset in base_offsets {
            self.new_segment(base_offset)?;
        }
        if self.segments.is_empty() {
            self.new_segment(self.config.segment.initial_offset)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn it_works() -> Result<()> {
        let dir = tempdir()?;
        let log = Log::new(dir.path(), Config::default())?;
        assert_eq!(1, log.segments.len());
        assert_eq!(Some(0), log.active_segment_idx);
        let log = Log::new(dir.path(), Config::default())?;
        assert_eq!(1, log.segments.len());
        assert_eq!(Some(0), log.active_segment_idx);

        Ok(())
    }
}
