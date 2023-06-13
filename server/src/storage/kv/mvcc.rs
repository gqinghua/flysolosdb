use std::{sync::RwLock, collections::HashSet, borrow::Cow, error::Error, ops::{RangeBounds, Bound}, iter::Peekable};

use bincode::serialize;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};


use super::{encoding, Range, Store};
use crate::error::error::Result;
pub struct MVCC {
    ///底层的KV存储。它受到互斥锁的保护，因此可以在txns之间共享。
    store: Arc<RwLock<Box<dyn Store>>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    pub txns: u64,
    pub txns_active: u64,
    pub storage: String,
}
impl Clone for MVCC {
    fn clone(&self) -> Self {
        MVCC { store: self.store.clone() }
    }
}



impl MVCC {
    ///使用给定的键值存储创建一个新的MVCC键值存储。
    pub fn new(store: Box<dyn Store>) -> Self {
        Self { store: Arc::new(RwLock::new(store)) }
    }

   ///以读写模式开始一个新事务。
    #[allow(dead_code)]
    pub fn begin(&self) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), Mode::ReadWrite)
    }

   ///以给定的模式开始一个新的事务。
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        Transaction::begin(self.store.clone(), mode)
    }

    ///使用给定的ID恢复事务。
    pub fn resume(&self, id: u64) -> Result<Transaction> {
        Transaction::resume(self.store.clone(), id)
    }

   ///获取未版本控制的元数据值
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        session.get(&Key::Metadata(key.into()).encode())
    }

   ///设置未版本控制的元数据值
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let mut session = self.store.write()?;
        session.set(&Key::Metadata(key.into()).encode(), value)
    }

   ///返回引擎状态// 
   /// //奇怪的是，返回语句实际上是必要的-参见:// https://github.com/rust-lang/reference/issues/452
    #[allow(clippy::needless_return)]
    pub fn status(&self) -> Result<Status> {
        let store = self.store.read()?;
        return Ok(Status {
            txns: match store.get(&Key::TxnNext.encode())? {
                Some(ref v) => deserialize(v)?,
                None => 1,
            } - 1,
            txns_active: store
                .scan(Range::from(
                    Key::TxnActive(0).encode()..Key::TxnActive(std::u64::MAX).encode(),
                ))
                .try_fold(0, |count, r| r.map(|_| count + 1))?,
            storage: store.to_string(),
        });
    }
}



///一个MVCC事务。
pub struct Transaction {
    ///事务的底层存储。使用互斥锁在事务之间共享。
    store: Arc<RwLock<Box<dyn Store>>>,
    ///唯一的事务ID。
    id: u64,
    /// 事务模式。
    mode: Mode,
    /// 事务正在其中运行的快照。
    snapshot: Snapshot,
}


impl Transaction {
    /// Begins a new transaction in the given mode.
    fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Result<Self> {
        let mut session = store.write()?;

        let id = match session.get(&Key::TxnNext.encode())? {
            Some(ref v) => deserialize(v)?,
            None => 1,
        };
        session.set(&Key::TxnNext.encode(), serialize(&(id + 1))?)?;
        session.set(&Key::TxnActive(id).encode(), serialize(&mode)?)?;

        // We always take a new snapshot, even for snapshot transactions, because all transactions
        // increment the transaction ID and we need to properly record currently active transactions
        // for any future snapshot transactions looking at this one.
        let mut snapshot = Snapshot::take(&mut session, id)?;
        std::mem::drop(session);
        if let Mode::Snapshot { version } = &mode {
            snapshot = Snapshot::restore(&store.read()?, *version)?
        }

        Ok(Self { store, id, mode, snapshot })
    }

    /// Resumes an active transaction with the given ID. Errors if the transaction is not active.
    fn resume(store: Arc<RwLock<Box<dyn Store>>>, id: u64) -> Result<Self> {
        let session = store.read()?;
        let mode = match session.get(&Key::TxnActive(id).encode())? {
            Some(v) => deserialize(&v)?,
            None => return Err(Error::Value(format!("No active transaction {}", id))),
        };
        let snapshot = match &mode {
            Mode::Snapshot { version } => Snapshot::restore(&session, *version)?,
            _ => Snapshot::restore(&session, id)?,
        };
        std::mem::drop(session);
        Ok(Self { store, id, mode, snapshot })
    }

    /// Returns the transaction ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the transaction mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Commits the transaction, by removing the txn from the active set.
    pub fn commit(self) -> Result<()> {
        let mut session = self.store.write()?;
        session.delete(&Key::TxnActive(self.id).encode())?;
        session.flush()
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(self) -> Result<()> {
        let mut session = self.store.write()?;
        if self.mode.mutable() {
            let mut rollback = Vec::new();
            let mut scan = session.scan(Range::from(
                Key::TxnUpdate(self.id, vec![].into()).encode()
                    ..Key::TxnUpdate(self.id + 1, vec![].into()).encode(),
            ));
            while let Some((key, _)) = scan.next().transpose()? {
                match Key::decode(&key)? {
                    Key::TxnUpdate(_, updated_key) => rollback.push(updated_key.into_owned()),
                    k => return Err(Error::Internal(format!("Expected TxnUpdate, got {:?}", k))),
                };
                rollback.push(key);
            }
            std::mem::drop(scan);
            for key in rollback.into_iter() {
                session.delete(&key)?;
            }
        }
        session.delete(&Key::TxnActive(self.id).encode())
    }

    /// Deletes a key.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.write(key, None)
    }

    /// Fetches a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let session = self.store.read()?;
        let mut scan = session
            .scan(Range::from(
                Key::Record(key.into(), 0).encode()..=Key::Record(key.into(), self.id).encode(),
            ))
            .rev();
        while let Some((k, v)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Record(_, version) => {
                    if self.snapshot.is_visible(version) {
                        return deserialize(&v);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            };
        }
        Ok(None)
    }

    /// Scans a key range.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<super::Scan> {
        let start = match range.start_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Included(Key::Record(vec![].into(), 0).encode()),
        };
        let end = match range.end_bound() {
            Bound::Excluded(k) => Bound::Excluded(Key::Record(k.into(), 0).encode()),
            Bound::Included(k) => Bound::Included(Key::Record(k.into(), std::u64::MAX).encode()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let scan = self.store.read()?.scan(Range::from((start, end)));
        Ok(Box::new(Scan::new(scan, self.snapshot.clone())))
    }

    /// Scans keys under a given prefix.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<super::Scan> {
        if prefix.is_empty() {
            return Err(Error::Internal("Scan prefix cannot be empty".into()));
        }
        let start = prefix.to_vec();
        let mut end = start.clone();
        for i in (0..end.len()).rev() {
            match end[i] {
                // If all 0xff we could in principle use Range::Unbounded, but it won't happen
                0xff if i == 0 => return Err(Error::Internal("Invalid prefix scan range".into())),
                0xff => {
                    end[i] = 0x00;
                    continue;
                }
                v => {
                    end[i] = v + 1;
                    break;
                }
            }
        }
        self.scan(start..end)
    }

    /// Sets a key.
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write(key, Some(value))
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::ReadOnly);
        }
        let mut session = self.store.write()?;

        // Check if the key is dirty, i.e. if it has any uncommitted changes, by scanning for any
        // versions that aren't visible to us.
        let min = self.snapshot.invisible.iter().min().cloned().unwrap_or(self.id + 1);
        let mut scan = session
            .scan(Range::from(
                Key::Record(key.into(), min).encode()
                    ..=Key::Record(key.into(), std::u64::MAX).encode(),
            ))
            .rev();
        while let Some((k, _)) = scan.next().transpose()? {
            match Key::decode(&k)? {
                Key::Record(_, version) => {
                    if !self.snapshot.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                }
                k => return Err(Error::Internal(format!("Expected Txn::Record, got {:?}", k))),
            };
        }
        std::mem::drop(scan);

        // Write the key and its update record.
        let key = Key::Record(key.into(), self.id).encode();
        let update = Key::TxnUpdate(self.id, (&key).into()).encode();
        session.set(&update, vec![])?;
        session.set(&key, serialize(&value)?)
    }
}


/// MVCC事务模式。
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Mode {
   ///读写事务。
    ReadWrite,
   ///只读事务。
    ReadOnly,
///在给定版本的快照中运行的只读事务。
///
///版本必须引用提交的事务ID。对原始文件可见的任何更改
///事务将在快照中可见(即之前没有提交的事务)
///启动的快照事务将不可见，即使它们有较低的版本)。
    Snapshot { version: u64 },
}

///包含并发事务可见性信息的版本快照。
#[derive(Clone)]
struct Snapshot {
     ///快照所属的版本(即事务ID)。
    version: u64,
     ///在事务开始时活动的事务id集合;
     ///在事务开始时活动的事务id集合;
    invisible: HashSet<u64>,
}


/// MVCC键。编码保留了键的分组和顺序。我们想用奶牛就用奶牛
///编码时取borrow，解码时返回owned。
#[derive(Debug)]
enum Key<'a> {
     ///下一个可用的txn ID。在启动新的txns时使用。
    TxnNext,
    ///激活txn标记，包含模式。用于检测并发txns，并恢复。
    TxnActive(u64),
    /// Txn快照，包含Txn开始时并发的活动Txn。
    TxnSnapshot(u64),
   ///用于回滚的txn ID和键的更新标记。
    TxnUpdate(u64, Cow<'a, [u8]>),
   ///键/版本对的记录。
    Record(Cow<'a, [u8]>, u64),
   ///任意未版本的元数据。
    Metadata(Cow<'a, [u8]>),
}

impl<'a> Key<'a> {
    /// Encodes a key into a byte vector.
    fn encode(self) -> Vec<u8> {
        use encoding::*;
        match self {
            Self::TxnNext => vec![0x01],
            Self::TxnActive(id) => [&[0x02][..], &encode_u64(id)].concat(),
            Self::TxnSnapshot(version) => [&[0x03][..], &encode_u64(version)].concat(),
            Self::TxnUpdate(id, key) => {
                [&[0x04][..], &encode_u64(id), &encode_bytes(&key)].concat()
            }
            Self::Metadata(key) => [&[0x05][..], &encode_bytes(&key)].concat(),
            Self::Record(key, version) => {
                [&[0xff][..], &encode_bytes(&key), &encode_u64(version)].concat()
            }
        }
    }

    /// Decodes a key from a byte representation.
    fn decode(mut bytes: &[u8]) -> Result<Self> {
        use encoding::*;
        let bytes = &mut bytes;
        let key = match take_byte(bytes)? {
            0x01 => Self::TxnNext,
            0x02 => Self::TxnActive(take_u64(bytes)?),
            0x03 => Self::TxnSnapshot(take_u64(bytes)?),
            0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
            0x05 => Self::Metadata(take_bytes(bytes)?.into()),
            0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
            b => return Err(Error::Internal(format!("Unknown MVCC key prefix {:x?}", b))),
        };
        if !bytes.is_empty() {
            return Err(Error::Internal("Unexpected data remaining at end of key".into()));
        }
        Ok(key)
    }
}

///反序列化MVCC元数据。
fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
    Ok(bincode::deserialize(bytes).unwrap())
}


///键范围扫描。
pub struct Scan {
///增强的KV存储迭代器，包含键(解码)和值。注意，我们没有保留
///解码后的版本，所以会有多个密钥(每个版本)。我们想要最后一个。
    scan: Peekable<super::Scan>,
    ///跟踪next_back()所看到的键，其以前的版本应该被忽略。
    next_back_seen: Option<Vec<u8>>,
}


impl Scan {
    /// Creates a new scan.
    fn new(mut scan: super::Scan, snapshot: Snapshot) -> Self {
        // Augment the underlying scan to decode the key and filter invisible versions. We don't
        // return the version, since we don't need it, but beware that all versions of the key
        // will still be returned - we usually only need the last, which is what the next() and
        // next_back() methods need to handle. We also don't decode the value, since we only need
        // to decode the last version.
        scan = Box::new(scan.filter_map(move |r| {
            r.and_then(|(k, v)| match Key::decode(&k)? {
                Key::Record(_, version) if !snapshot.is_visible(version) => Ok(None),
                Key::Record(key, _) => Ok(Some((key.into_owned(), v))),
                k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
            })
            .transpose()
        }));
        Self { scan: scan.peekable(), next_back_seen: None }
    }

    // next() with error handling.
    fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next().transpose()? {
            // Only return the item if it is the last version of the key.
            if match self.scan.peek() {
                Some(Ok((peek_key, _))) if *peek_key != key => true,
                Some(Ok(_)) => false,
                Some(Err(err)) => return Err(err.clone()),
                None => true,
            } {
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }

    /// next_back() with error handling.
    fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        while let Some((key, value)) = self.scan.next_back().transpose()? {
            // Only return the last version of the key (so skip if seen).
            if match &self.next_back_seen {
                Some(seen_key) if *seen_key != key => true,
                Some(_) => false,
                None => true,
            } {
                self.next_back_seen = Some(key.clone());
                // Only return non-deleted items.
                if let Some(value) = deserialize(&value)? {
                    return Ok(Some((key, value)));
                }
            }
        }
        Ok(None)
    }
}
