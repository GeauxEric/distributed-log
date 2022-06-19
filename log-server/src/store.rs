use std::fs::File;
use std::io;
use std::io::{BufWriter, Write};
use std::os::unix::fs::FileExt;
use std::sync::Mutex;

const LEN_WIDTH: u64 = 8;

struct Store<'s> {
    mu: Mutex<()>,
    file: &'s File,           // read
    buf: BufWriter<&'s File>, // write
    size: u64,
}

impl<'s> Store<'s> {
    pub fn new(file: &'s File) -> io::Result<Store<'s>> {
        let m = file.metadata()?;
        Ok(Store {
            mu: Mutex::new(()),
            file,
            buf: BufWriter::new(file),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempfile;

    #[test]
    fn test_store() {
        let file = tempfile().unwrap();

        let mut store = Store::new(&file).unwrap();
        let r = store.append(&[1, 2, 3]);
        assert!(r.is_ok());
        let r = r.unwrap();
        assert_eq!(r.0, 11);
        assert_eq!(r.1, 0);

        let r = store.read(r.1).unwrap();
        assert_eq!(&r, &[1, 2, 3]);
    }
}
