use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::sync::Mutex;

const LEN_WIDTH: u64 = 8;

pub(crate) struct Store {
    mu: Mutex<()>,
    file: File,           // read
    buf: BufWriter<File>, // write
    size: u64,
}

impl Store {
    pub fn new(file: File) -> io::Result<Store> {
        let m = file.metadata()?;
        let write_fd = file.try_clone()?;
        Ok(Store {
            mu: Mutex::new(()),
            file,
            buf: BufWriter::new(write_fd),
            size: m.len(),
        })
    }

    pub fn append(&mut self, p: &[u8]) -> io::Result<(u64, u64)> {
        let _l = self.mu.lock().unwrap();
        let pos = self.size;
        let b = (p.len() as u64).to_le_bytes() as [u8; LEN_WIDTH as usize];
        let buf = &mut self.buf;
        buf.write_all(&b)?;
        let mut w = buf.write(p)? as u64;
        w += LEN_WIDTH;
        self.size += w;
        Ok((w, pos))
    }

    pub fn read(&mut self, pos: u64) -> io::Result<Vec<u8>> {
        let _l = self.mu.lock().unwrap();
        self.buf.flush()?;

        let mut b = [0u8; LEN_WIDTH as usize];
        self.file.read_exact_at(&mut b, pos)?;
        let sz = u64::from_le_bytes(b) as usize;
        let mut b = vec![0; sz];
        self.file.read_exact_at(&mut b, pos + LEN_WIDTH)?;
        Ok(b)
    }

    pub fn read_exact_at(&mut self, buf: &mut [u8], pos: u64) -> io::Result<()> {
        let _l = self.mu.lock().unwrap();
        self.buf.flush()?;
        self.file.read_exact_at(buf, pos)
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let _l = self.mu.lock().unwrap();
        self.buf.flush().expect("Store bufwriter failed to flush");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    #[test]
    fn test_store() {
        let file = tempfile().unwrap();
        let mut store = Store::new(file).unwrap();
        let r = store.append(&[1, 2, 3]);
        assert!(r.is_ok());
        let r = r.unwrap();
        assert_eq!(r.0, 11);
        assert_eq!(r.1, 0);

        let read = store.read(r.1).unwrap();
        assert_eq!(&read, &[1, 2, 3]);

        let mut ba = [0u8; LEN_WIDTH as usize];
        store.read_exact_at(&mut ba, r.1).unwrap();
        let width = u64::from_le_bytes(ba);
        assert_eq!(width, 3);
    }
}
