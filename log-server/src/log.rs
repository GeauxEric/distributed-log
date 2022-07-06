use std::collections::HashSet;
use std::fs::read_dir;
use std::path::{Path, PathBuf};
use std::sync;

use anyhow::{anyhow, Result};
use log::debug;

use protos::log::v1::Record;

use crate::config::Config;
use crate::segment::Segment;

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
        let s = Segment::new(&self.dir, off, &self.config)?;
        self.segments.push(s);
        self.active_segment_idx = Some(self.segments.len() - 1);
        Ok(())
    }

    fn append(&mut self, record: &mut Record) -> Result<u64> {
        let _l = self.lock.get_mut().expect("failed to get mutable lock");
        if self.active_segment_idx.is_none() {
            return Err(anyhow!("there is not active segment"));
        }
        let s = self
            .segments
            .get_mut(self.active_segment_idx.unwrap())
            .expect("no segment at the active segment idx");
        let offset = s.append(record)?;
        if s.is_maxed() {
            self.new_segment(offset + 1)
                .expect("error adding new segment");
        }
        Ok(offset)
    }

    fn read(&self, off: u64) -> Result<Record> {
        let _l = self.lock.read().unwrap();
        let s = self
            .segments
            .iter()
            .find(|&s| s.base_offset <= off && s.next_offset > off)
            .ok_or_else(|| anyhow!(format!("offset={} is out of range", off)))?;
        s.read(off)
    }

    fn lowest_offset(&self) -> Result<u64> {
        let _l = self.lock.read().unwrap();
        let s = self
            .segments
            .get(0)
            .ok_or_else(|| anyhow!("segments is empty"))?;
        Ok(s.base_offset)
    }

    fn close(&mut self) -> Result<()> {
        let _l = self.lock.write().unwrap();
        for s in &mut self.segments {
            s.close()?
        }
        Ok(())
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
            debug!("init from base_offsets={}", base_offset);
            self.new_segment(base_offset)?;
        }
        if self.segments.is_empty() {
            debug!("create new segment");
            self.new_segment(self.config.segment.initial_offset)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn it_works() -> Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let dir = tempdir()?;
        let mut log = Log::new(dir.path(), Config::default())?;
        assert_eq!(1, log.segments.len());
        assert_eq!(Some(0), log.active_segment_idx);
        log.close()?;

        let mut log = Log::new(dir.path(), Config::default())?;
        assert_eq!(1, log.segments.len());
        assert_eq!(Some(0), log.active_segment_idx);

        test_append_and_read(&mut log)?;
        test_out_of_range(&log)?;
        test_init_existing(&mut log)?;

        Ok(())
    }

    fn test_append_and_read(log: &mut Log) -> Result<()> {
        let mut r1 = Record {
            value: vec![1, 2, 3],
            ..Default::default()
        };
        let offset = log.append(&mut r1)?;
        let g1 = log.read(offset)?;
        assert_eq!(r1.value, g1.value);
        Ok(())
    }

    fn test_out_of_range(log: &Log) -> Result<()> {
        let r = log.read(1);
        assert!(r.is_err());
        assert!(r.err().unwrap().to_string().contains("out of range"));
        Ok(())
    }

    fn test_init_existing(log: &mut Log) -> Result<()> {
        for _i in 0..3 {
            let mut r1 = Record {
                value: "hello world".to_owned().into_bytes(),
                ..Default::default()
            };
            log.append(&mut r1)?;
        }
        log.close()?;
        let off = log.lowest_offset()?;
        assert_eq!(0, off);

        // TODO: highest offset
        Ok(())
    }
}
