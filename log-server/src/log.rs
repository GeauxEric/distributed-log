use std::sync::Mutex;

#[derive(PartialEq, Debug)]
enum LogErr {
    OffsetNotFound,
}

#[derive(Clone, PartialEq, Debug, Default)]
struct Record {
    pub value: Vec<u8>,

    pub offset: u64,
}

struct Log {
    records: Mutex<Vec<Record>>,
}

impl Log {
    pub fn new() -> Log {
        Log {
            records: Mutex::new(vec![]),
        }
    }

    pub fn append(&mut self, mut record: Record) -> u64 {
        let l = self.records.get_mut().unwrap();
        let offset = l.len() as u64;
        record.offset = offset;
        l.push(record);
        offset
    }

    pub fn read(&self, offset: u64) -> Result<Record, LogErr> {
        let l = self.records.lock().unwrap();
        match l.get(offset as usize) {
            Some(r) => Ok(r.clone()),
            None => Err(LogErr::OffsetNotFound),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log() {
        let mut l = Log::new();
        assert_eq!(
            l.append(Record {
                value: vec![1],
                ..Default::default()
            }),
            0
        );
        assert_eq!(
            l.append(Record {
                value: vec![1],
                ..Default::default()
            }),
            1
        );
        assert_eq!(l.read(2), Err(LogErr::OffsetNotFound));
    }
}
