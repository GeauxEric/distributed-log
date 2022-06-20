use crate::config::Config;
use memmap::MmapMut;
use std::fs::File;
use std::io::{ErrorKind, Read, Write};

const OFF_WIDTH: usize = 4;
const POS_WIDTH: usize = 8;
const ENTRY_WIDTH: usize = OFF_WIDTH + POS_WIDTH;

struct Index<'i> {
    file: &'i File,
    size: u64,
    mmap: MmapMut,
}

impl<'i> Index<'i> {
    pub fn new(file: &'i File, config: &Config) -> std::io::Result<Self> {
        let sz = file.metadata()?.len();
        file.set_len(config.segment.max_index_bytes)?;
        let mmap = unsafe { MmapMut::map_mut(file).unwrap() };
        Ok(Index {
            file,
            size: sz,
            mmap,
        })
    }

    pub fn write(&mut self, off: u32, pos: u64) -> std::io::Result<()> {
        if self.mmap.len() < (self.size + ENTRY_WIDTH as u64) as usize {
            return Err(std::io::Error::new(ErrorKind::UnexpectedEof, ""));
        }
        let sz = self.size as usize;
        (&mut self.mmap[sz..sz + 100]).write_all(off.to_le_bytes().as_slice())?;
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
}

impl<'i> Drop for Index<'i> {
    fn drop(&mut self) {
        self.mmap.flush().expect("Index mmap failed to flush");
        self.file.flush().expect("Index file failed to flush");
        self.file
            .set_len(self.size)
            .expect("Index file failed to truncate");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Segment;
    use tempfile::tempfile;

    #[test]
    fn test_index() {
        let file = tempfile().unwrap();
        let config = Config {
            segment: Segment {
                max_index_bytes: 1024,
                ..Default::default()
            },
        };
        let mut index = Index::new(&file, &config).unwrap();
        assert!(index.read(-1).is_err());

        struct Entry {
            off: u32,
            pos: u64,
        }

        for e in &vec![Entry { off: 0, pos: 0 }, Entry { off: 0, pos: 0 }] {
            assert!(index.write(e.off, e.pos).is_ok());

            let t = index.read(e.off as i64).unwrap();
            assert_eq!(t.0, e.off);
            assert_eq!(t.1, e.pos);
        }
        assert_eq!(index.size, 2 * ENTRY_WIDTH as u64);
    }
}
