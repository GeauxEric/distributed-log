use crate::config::Config;
use crate::segment::Segment;
use anyhow::{anyhow, Result};
use std::collections::HashSet;
use std::fs::read_dir;
use std::path::{Path, PathBuf};

struct Log {
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
