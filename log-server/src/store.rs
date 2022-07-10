use std::cell::RefCell;
use std::fs::File;
use std::io;
use std::io::{BufWriter, Read, Write};
use std::os::unix::fs::FileExt;
use std::sync::Mutex;

pub(crate) const LEN_WIDTH: u64 = 8;

pub(crate) struct Store {
    mu: Mutex<()>,
    file: File,                    // read
    buf: RefCell<BufWriter<File>>, // write
    size: u64,
}

impl Store {
    pub fn new(file: File) -> io::Result<Store> {
        let m = file.metadata()?;
        let write_fd = file.try_clone()?;
        Ok(Store {
            mu: Mutex::new(()),
            file,
            buf: RefCell::new(BufWriter::new(write_fd)),
            size: m.len(),
        })
    }

    pub fn close(&mut self) -> anyhow::Result<()> {
        let _l = self.mu.lock().unwrap();
        self.buf.borrow_mut().flush()?;
        Ok(())
    }

    pub fn append(&mut self, p: &[u8]) -> io::Result<(u64, u64)> {
        let _l = self.mu.lock().unwrap();
        let pos = self.size;
        let b = (p.len() as u64).to_le_bytes() as [u8; LEN_WIDTH as usize];
        let buf = &mut self.buf;
        buf.borrow_mut().write_all(&b)?;
        let mut w = buf.borrow_mut().write(p)? as u64;
        w += LEN_WIDTH;
        self.size += w;
        Ok((w, pos))
    }

    pub fn read(&self, pos: u64) -> io::Result<Vec<u8>> {
        let _l = self.mu.lock().unwrap();
        self.buf.borrow_mut().flush()?;

        let mut b = [0u8; LEN_WIDTH as usize];
        self.file.read_exact_at(&mut b, pos)?;
        let sz = u64::from_le_bytes(b) as usize;
        let mut b = vec![0; sz];
        self.file.read_exact_at(&mut b, pos + LEN_WIDTH)?;
        Ok(b)
    }

    pub fn read_at(&self, buf: &mut [u8], pos: u64) -> io::Result<usize> {
        let _l = self.mu.lock().unwrap();
        self.buf.borrow_mut().flush()?;
        self.file.read_at(buf, pos)
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.close().expect("failed to close")
    }
}

pub(crate) struct StoreReader<'a> {
    pub(crate) store: &'a Store,
    pub(crate) off: u64,
}

impl<'a> Read for StoreReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.store.read_at(buf, self.off)?;
        self.off += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multi_reader::MultiReader;
    use std::collections::VecDeque;
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
        store.read_at(&mut ba, r.1).unwrap();
        let width = u64::from_le_bytes(ba);
        assert_eq!(width, 3);
    }

    #[test]
    fn store_reader() {
        let f1 = tempfile().unwrap();
        let mut store1 = Store::new(f1).unwrap();
        store1.append(&[1, 1, 1, 1]).expect("");
        store1.append(&[2, 2, 2, 2]).expect("");
        let mut sr1 = StoreReader {
            store: &store1,
            off: 0,
        };

        let mut buf = [0u8; 12];
        let n1 = sr1.read(&mut buf).expect("");
        assert_eq!(n1, 12);
        let n1 = sr1.read(&mut buf).expect("");
        assert_eq!(n1, 12);
        let n1 = sr1.read(&mut buf).expect("");
        assert_eq!(n1, 0);
    }

    #[test]
    fn multi_store_reader() {
        let f1 = tempfile().unwrap();
        let mut store1 = Store::new(f1).unwrap();
        store1.append(&[1, 1, 1, 1]).expect("");

        let f2 = tempfile().unwrap();
        let mut store2 = Store::new(f2).unwrap();
        store2.append(&[2, 2, 2, 2]).expect("");

        let sr1 = StoreReader {
            store: &store1,
            off: 0,
        };
        let sr2 = StoreReader {
            store: &store2,
            off: 0,
        };

        let mut mr = MultiReader {
            inner: VecDeque::new(),
        };
        mr.inner.push_back(sr1);
        mr.inner.push_back(sr2);

        let mut b = [0u8; (8 + 4)];
        for i in 0..3 {
            let n = mr.read(&mut b).expect("");
            if i == 0 {
                assert_eq!(n, 12);
            }
            if i == 1 {
                assert_eq!(n, 12);
            }
            if i == 2 {
                assert_eq!(n, 0);
            }
        }
    }
}
