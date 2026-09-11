use crate::{BustubxError, BustubxResult};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"BUSTXJ01";
const COMMIT: &[u8; 8] = b"COMMIT01";
const HEADER: usize = 40;

pub(super) struct Journal {
    file: File,
    failed: bool,
}

fn checksum(version: u64, payload: &[u8]) -> u64 {
    version
        .to_le_bytes()
        .into_iter()
        .chain((payload.len() as u64).to_le_bytes())
        .chain(payload.iter().copied())
        .fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ byte as u64).wrapping_mul(0x100000001b3)
        })
}

fn record(version: u64, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(MAGIC);
    bytes.extend(version.to_le_bytes());
    bytes.extend((payload.len() as u64).to_le_bytes());
    bytes.extend(checksum(version, payload).to_le_bytes());
    bytes.extend(checksum(version, &(payload.len() as u64).to_le_bytes()).to_le_bytes());
    bytes.extend(payload);
    bytes.extend(COMMIT);
    bytes
}

impl Journal {
    pub fn open(path: impl AsRef<Path>) -> BustubxResult<(Self, Option<(u64, Vec<u8>)>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.try_lock().map_err(|error| {
            BustubxError::Storage(format!(
                "Cannot exclusively lock transaction journal: {error}"
            ))
        })?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let mut cursor = 0;
        let mut latest = None;
        while bytes.len() - cursor >= HEADER {
            let header = &bytes[cursor..cursor + HEADER];
            if &header[..8] != MAGIC {
                return Err(BustubxError::Storage("Invalid journal record magic".into()));
            }
            let version = u64::from_le_bytes(header[8..16].try_into().unwrap());
            let length = u64::from_le_bytes(header[16..24].try_into().unwrap());
            let expected = u64::from_le_bytes(header[24..32].try_into().unwrap());
            let header_checksum = u64::from_le_bytes(header[32..40].try_into().unwrap());
            if header_checksum != checksum(version, &length.to_le_bytes()) {
                return Err(BustubxError::Storage(
                    "Corrupt journal record header".into(),
                ));
            }
            let available = bytes.len() - cursor - HEADER;
            if length > available.saturating_sub(COMMIT.len()) as u64 || available < COMMIT.len() {
                break;
            }
            let end = cursor + HEADER + length as usize;
            let payload = &bytes[cursor + HEADER..end];
            if &bytes[end..end + 8] != COMMIT || checksum(version, payload) != expected {
                return Err(BustubxError::Storage(
                    "Corrupt committed journal record".into(),
                ));
            }
            let expected_version = match &latest {
                None => 0,
                Some((previous, _)) => u64::checked_add(*previous, 1)
                    .ok_or_else(|| BustubxError::Storage("Journal version overflow".into()))?,
            };
            if version != expected_version {
                return Err(BustubxError::Storage(
                    "Non-contiguous journal versions".into(),
                ));
            }
            latest = Some((version, payload.to_vec()));
            cursor = end + COMMIT.len();
        }
        if !bytes.is_empty() && latest.is_none() {
            return Err(BustubxError::Storage(
                "Journal has no complete initial record".into(),
            ));
        }
        if cursor < bytes.len() {
            file.set_len(cursor as u64)?;
            file.sync_all()?;
        }
        Ok((
            Self {
                file,
                failed: false,
            },
            latest,
        ))
    }

    pub fn ensure_usable(&self) -> BustubxResult<()> {
        if self.failed {
            return Err(BustubxError::Transaction(
                "Journal write failed; reopen to determine commit outcome".into(),
            ));
        }
        Ok(())
    }

    pub fn append(&mut self, version: u64, payload: &[u8]) -> BustubxResult<()> {
        self.ensure_usable()?;
        let bytes = record(version, payload);
        let result = (|| -> std::io::Result<()> {
            self.file.seek(SeekFrom::End(0))?;
            self.file.write_all(&bytes)?;
            self.file.sync_all()
        })();
        if let Err(error) = result {
            self.failed = true;
            return Err(BustubxError::Transaction(format!(
                "Journal write failed ({error}); reopen to determine commit outcome"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_incomplete_tail_recovers_the_previous_commit() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("journal");
        let initial = record(0, b"initial");
        let next = record(1, b"committed database image");
        for cut in 1..next.len() {
            let mut bytes = initial.clone();
            bytes.extend(&next[..cut]);
            std::fs::write(&path, bytes).unwrap();
            let (mut journal, recovered) = Journal::open(&path).unwrap();
            assert_eq!(recovered.unwrap(), (0, b"initial".to_vec()));
            assert_eq!(journal.file.metadata().unwrap().len(), initial.len() as u64);
            journal.append(1, b"new commit").unwrap();
            drop(journal);
            assert_eq!(
                Journal::open(&path).unwrap().1.unwrap(),
                (1, b"new commit".to_vec())
            );
        }
    }

    #[test]
    fn corruption_and_concurrent_open_fail_without_rewriting_history() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("journal");
        let mut bytes = record(0, b"initial");
        bytes[HEADER] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert!(Journal::open(&path).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        std::fs::write(&path, record(1, b"wrong first version")).unwrap();
        assert!(Journal::open(&path).is_err());
        std::fs::write(&path, record(0, b"valid")).unwrap();
        let (journal, _) = Journal::open(&path).unwrap();
        assert!(Journal::open(&path).is_err());
        drop(journal);
        assert!(Journal::open(&path).is_ok());
    }
}
