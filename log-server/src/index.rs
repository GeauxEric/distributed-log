use std::fs::File;
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

use log::debug;
use memmap::MmapMut;

use crate::config::Config;

const OFF_WIDTH: usize = 4;
const POS_WIDTH: usize = 8;
pub(crate) const ENTRY_WIDTH: usize = OFF_WIDTH + POS_WIDTH;

pub(crate) struct Index {
    file: File,
    /// [`PathBuf`] of the file
    pub(crate) file_path: Option<PathBuf>,
    size: u64,
    mmap: MmapMut,
}

impl Index {
    pub fn new(file: File, config: &Config) -> std::io::Result<Self> {
        let size = file.metadata()?.len();
        file.set_len(config.segment.max_index_bytes)?;
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        Ok(Index {
            file,
            file_path: None,
            size,
            mmap,
        })
    }

    pub fn with_path(mut self, path: &Path) -> Self {
        self.file_path = Some(path.to_path_buf());
        self
    }

    pub fn write(&mut self, off: u32, pos: u64) -> std::io::Result<()> {
        let s = self.size + ENTRY_WIDTH as u64;
        let mmap_len = self.mmap.len();
        if mmap_len < s as usize {
            return Err(std::io::Error::new(
                ErrorKind::UnexpectedEof,
                format!("mmap length {} is less than {}", mmap_len, s),
            ));
        }
        let sz = self.size as usize;
        (&mut self.mmap[sz..sz + OFF_WIDTH]).write_all(off.to_le_bytes().as_slice())?;
        (&mut self.mmap[sz + OFF_WIDTH..sz + ENTRY_WIDTH])
            .write_all(pos.to_le_bytes().as_slice())?;
        self.size += ENTRY_WIDTH as u64;
        Ok(())
    }

    pub fn read(&self, offset: i64) -> std::io::Result<(u32, u64)> {
        if self.size == 0 {
            return Err(std::io::Error::new(ErrorKind::UnexpectedEof, ""));
        }
        let out = {
            if offset == -1 {
                ((self.size / ENTRY_WIDTH as u64) - 1) as u32
            } else {
                offset as u32
            }
        };
        let pos = (out as usize * ENTRY_WIDTH) as usize;
        let mut ba = [0u8; OFF_WIDTH];
        (&self.mmap[pos..pos + OFF_WIDTH]).read_exact(&mut ba)?;
        let out = u32::from_le_bytes(ba);
        let mut ba = [0u8; POS_WIDTH];
        (&self.mmap[pos + OFF_WIDTH..pos + ENTRY_WIDTH]).read_exact(&mut ba)?;
        let pos = u64::from_le_bytes(ba);
        Ok((out, pos))
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn close(&mut self) -> anyhow::Result<()> {
        self.mmap.flush().expect("Index mmap failed to flush");
        self.file.flush().expect("Index file failed to flush");
        debug!("dropping index file and truncate to size={}", self.size);
        self.file.set_len(self.size)?;
        Ok(())
    }
}

impl Drop for Index {
    fn drop(&mut self) {
        self.close().expect("index file fail to close")
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempfile;

    use crate::config::SegmentConfig;

    use super::*;

    #[test]
    fn test_index() {
        let file = tempfile().unwrap();
        let config = Config {
            segment: SegmentConfig {
                max_index_bytes: 1024,
                ..Default::default()
            },
        };
        let mut index = Index::new(file, &config).unwrap();
        assert!(index.read(-1).is_err());

        struct Entry {
            off: u32,
            pos: u64,
        }

        for e in &vec![Entry { off: 0, pos: 0 }, Entry { off: 1, pos: 10 }] {
            assert!(index.write(e.off, e.pos).is_ok());

            let t = index.read(e.off as i64).unwrap();
            assert_eq!(t.0, e.off);
            assert_eq!(t.1, e.pos);
        }
        assert_eq!(index.size, 2 * ENTRY_WIDTH as u64);

        let (out, pos) = index.read(-1).unwrap();
        assert_eq!(out, 1);
        assert_eq!(pos, 10);
    }
}
