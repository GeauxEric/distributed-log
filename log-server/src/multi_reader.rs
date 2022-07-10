use std::collections::VecDeque;
use std::io::Read;

pub(crate) struct MultiReader<R> {
    pub(crate) inner: VecDeque<R>,
}

impl<R> Default for MultiReader<R> {
    fn default() -> Self {
        MultiReader {
            inner: VecDeque::new(),
        }
    }
}

impl<R> Read for MultiReader<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while !self.inner.is_empty() {
            let r = self.inner.get_mut(0).unwrap();
            let sz = r.read(buf)?;
            if sz == 0 {
                self.inner.pop_front();
                continue;
            } else {
                return Ok(sz);
            }
        }

        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn multi_string_reader() {
        let s1 = "1234";
        let s2 = "56789";

        let c1 = Cursor::new(s1.as_bytes());
        let c2 = Cursor::new(s2.as_bytes());
        let mut mr = MultiReader {
            inner: VecDeque::new(),
        };
        mr.inner.push_back(c1);
        mr.inner.push_back(c2);

        let mut b1 = [0; 2];
        for i in 0..6 {
            let n = mr.read(&mut b1).expect("read from multi-reader");
            if i < 4 {
                assert_eq!(n, 2);
            }
            if i == 4 {
                assert_eq!(n, 1);
            }
            if i == 5 {
                assert_eq!(n, 0);
            }
        }
    }
}
