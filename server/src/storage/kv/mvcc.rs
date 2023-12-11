// use std::{sync::RwLock, collections::HashSet, borrow::Cow, errors::Error, ops::{RangeBounds, Bound}, iter::Peekable};
//
// use bincode::serialize;
// use serde::{Deserialize, Serialize};
// use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};
//
//
// use super::{encoding, Range, Store};
// use crate::errors::errors::{Result, Results};
// use crate::errors::errors::Errors;
// pub struct MVCC {
//     ///底层的KV存储。它受到互斥锁的保护，因此可以在txns之间共享。
//     store: Arc<RwLock<Box<dyn Store>>>,
// }
//
// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
// pub struct Status {
//     pub txns: u64,
//     pub txns_active: u64,
//     pub storage: String,
// }
// impl Clone for MVCC {
//     fn clone(&self) -> Self {
//         MVCC { store: self.store.clone() }
//     }
// }
//
//
//
// impl MVCC {
//     ///使用给定的键值存储创建一个新的MVCC键值存储。
//     pub fn new(store: Box<dyn Store>) -> Self {
//         Self { store: Arc::new(RwLock::new(store)) }
//     }
//
//    ///以读写模式开始一个新事务。
//     #[allow(dead_code)]
//     pub fn begin(&self) -> Result<Transaction> {
//         Transaction::begin(self.store.clone(), Mode::ReadWrite)
//     }
//
//    ///以给定的模式开始一个新的事务。
//     pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
//         Transaction::begin(self.store.clone(), mode)
//     }
//
//     ///使用给定的ID恢复事务。
//     pub fn resume(&self, id: u64) -> Result<Transaction> {
//         Transaction::resume(self.store.clone(), id)
//     }
//
//    ///获取未版本控制的元数据值
//     pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
//         let session = self.store.read()?;
//         session.get(&Key::Metadata(key.into()).encode())
//     }
//
//    ///设置未版本控制的元数据值
//     pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
//         let mut session = self.store.write()?;
//         session.set(&Key::Metadata(key.into()).encode(), value)
//     }
//
//    ///返回引擎状态//
//    /// //奇怪的是，返回语句实际上是必要的-参见:// https://github.com/rust-lang/reference/issues/452
//     #[allow(clippy::needless_return)]
//     pub fn status(&self) -> Result<Status> {
//         let store = self.store.read()?;
//         return Ok(Status {
//             txns: match store.get(&Key::TxnNext.encode())? {
//                 Some(ref v) => deserialize(v)?,
//                 None => 1,
//             } - 1,
//             txns_active: store
//                 .scan(Range::from(
//                     Key::TxnActive(0).encode()..Key::TxnActive(std::u64::MAX).encode(),
//                 ))
//                 .try_fold(0, |count, r| r.map(|_| count + 1))?,
//             storage: store.to_string(),
//         });
//     }
// }
//
//
//
// ///一个MVCC事务。
// pub struct Transaction {
//     ///事务的底层存储。使用互斥锁在事务之间共享。
//     store: Arc<RwLock<Box<dyn Store>>>,
//     ///唯一的事务ID。
//     id: u64,
//     /// 事务模式。
//     mode: Mode,
//     /// 事务正在其中运行的快照。
//     snapshot: Snapshot,
// }
//
//
// impl Transaction {
//     ///以给定的模式开始一个新的事务。
//     fn begin(store: Arc<RwLock<Box<dyn Store>>>, mode: Mode) -> Results<Self> {
//         let mut session = store.write().unwrap();
//
//         let id = match session.get(&Key::TxnNext.encode()) .unwrap(){
//             Some(ref v) => deserialize(v).unwrap(),
//             None => 1,
//         };
//         session.set(&Key::TxnNext.encode(), serialize(&(id + 1)).unwrap()).unwrap();
//         session.set(&Key::TxnActive(id).encode(), serialize(&mode).unwrap()).unwrap();
//
//         // We always take a new snapshot, even for snapshot transactions, because all transactions
//         // increment the transaction ID and we need to properly record currently active transactions
//         // for any future snapshot transactions looking at this one.
//         let mut snapshot = Snapshot::take(&mut session, id);
//         std::mem::drop(session);
//         if let Mode::Snapshot { version } = &mode {
//             snapshot = Snapshot::restore(&store.read().unwrap(), *version);
//         }
//
//         Ok(Self { store, id, mode, snapshot })
//     }
//
//
// }
//
//
// /// MVCC事务模式。
// #[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
// pub enum Mode {
//    ///读写事务。
//     ReadWrite,
//    ///只读事务。
//     ReadOnly,
// ///在给定版本的快照中运行的只读事务。
// ///
// ///版本必须引用提交的事务ID。对原始文件可见的任何更改
// ///事务将在快照中可见(即之前没有提交的事务)
// ///启动的快照事务将不可见，即使它们有较低的版本)。
//     Snapshot { version: u64 },
// }
//
// ///包含并发事务可见性信息的版本快照。
// #[derive(Clone)]
// struct Snapshot {
//      ///快照所属的版本(即事务ID)。
//     version: u64,
//      ///在事务开始时活动的事务id集合;
//      ///在事务开始时活动的事务id集合;
//     invisible: HashSet<u64>,
// }
//
//
// impl Snapshot {
//    ///获取一个新的快照，并将其持久化为' Key::TxnSnapshot(version) '。
//     fn take(session: &mut RwLockWriteGuard<Box<dyn Store>>, version: u64) -> Results<Self> {
//         let mut snapshot = Self { version, invisible: HashSet::new() };
//         let mut scan =
//             session.scan(Range::from(Key::TxnActive(0).encode()..Key::TxnActive(version).encode()));
//         while let Some((key, _)) = scan.next().transpose().unwrap() {
//             match Key::decode(&key).unwrap() {
//                 Key::TxnActive(id) => snapshot.invisible.insert(id),
//                 k => return Err(Errors::Internal(format!("Expected TxnActive, got {:?}", k))),
//             };
//         }
//         std::mem::drop(scan);
//         session.set(&Key::TxnSnapshot(version).encode(), serialize(&snapshot.invisible).unwrap());
//         Ok(snapshot)
//     }
//
//    ///从' Key::TxnSnapshot(version) '恢复现有快照，如果没有找到则返回错误。
//     fn restore(session: &RwLockReadGuard<Box<dyn Store>>, version: u64) -> Results<Self> {
//         match session.get(&Key::TxnSnapshot(version).encode()).unwrap() {
//             Some(ref v) => Ok(Self { version, invisible: deserialize(v).unwrap() }),
//             None => Err(Errors::Value(format!("Snapshot not found for version {}", version))),
//         }
//     }
//
//    ///检查给定的版本是否在此快照中可见。
//     fn is_visible(&self, version: u64) -> bool {
//         version <= self.version && self.invisible.get(&version).is_none()
//     }
// }
//
// /// MVCC键。编码保留了键的分组和顺序。我们想用奶牛就用奶牛
// ///编码时取borrow，解码时返回owned。
// #[derive(Debug)]
// enum Key<'a> {
//      ///下一个可用的txn ID。在启动新的txns时使用。
//     TxnNext,
//     ///激活txn标记，包含模式。用于检测并发txns，并恢复。
//     TxnActive(u64),
//     /// Txn快照，包含Txn开始时并发的活动Txn。
//     TxnSnapshot(u64),
//    ///用于回滚的txn ID和键的更新标记。
//     TxnUpdate(u64, Cow<'a, [u8]>),
//    ///键/版本对的记录。
//     Record(Cow<'a, [u8]>, u64),
//    ///任意未版本的元数据。
//     Metadata(Cow<'a, [u8]>),
// }
//
// impl<'a> Key<'a> {
//     /// Encodes a key into a byte vector.
//     fn encode(self) -> Vec<u8> {
//         use encoding::*;
//         match self {
//             Self::TxnNext => vec![0x01],
//             Self::TxnActive(id) => [&[0x02][..], &encode_u64(id)].concat(),
//             Self::TxnSnapshot(version) => [&[0x03][..], &encode_u64(version)].concat(),
//             Self::TxnUpdate(id, key) => {
//                 [&[0x04][..], &encode_u64(id), &encode_bytes(&key)].concat()
//             }
//             Self::Metadata(key) => [&[0x05][..], &encode_bytes(&key)].concat(),
//             Self::Record(key, version) => {
//                 [&[0xff][..], &encode_bytes(&key), &encode_u64(version)].concat()
//             }
//         }
//     }
//
//     /// Decodes a key from a byte representation.
//     fn decode(mut bytes: &[u8]) -> Result<Self> {
//         use encoding::*;
//         let bytes = &mut bytes;
//         let key = match take_byte(bytes)? {
//             0x01 => Self::TxnNext,
//             0x02 => Self::TxnActive(take_u64(bytes)?),
//             0x03 => Self::TxnSnapshot(take_u64(bytes)?),
//             0x04 => Self::TxnUpdate(take_u64(bytes)?, take_bytes(bytes)?.into()),
//             0x05 => Self::Metadata(take_bytes(bytes)?.into()),
//             0xff => Self::Record(take_bytes(bytes)?.into(), take_u64(bytes)?),
//             b => return Err(Error::Internal(format!("Unknown MVCC key prefix {:x?}", b))),
//         };
//         if !bytes.is_empty() {
//             return Err(Error::Internal("Unexpected data remaining at end of key".into()));
//         }
//         Ok(key)
//     }
// }
//
// ///反序列化MVCC元数据。
// fn deserialize<'a, V: Deserialize<'a>>(bytes: &'a [u8]) -> Result<V> {
//     Ok(bincode::deserialize(bytes).unwrap())
// }
//
//
// ///键范围扫描。
// pub struct Scan {
// ///增强的KV存储迭代器，包含键(解码)和值。注意，我们没有保留
// ///解码后的版本，所以会有多个密钥(每个版本)。我们想要最后一个。
//     scan: Peekable<super::Scan>,
//     ///跟踪next_back()所看到的键，其以前的版本应该被忽略。
//     next_back_seen: Option<Vec<u8>>,
// }
//
//
// impl Scan {
//     /// Creates a new scan.
//     fn new(mut scan: super::Scan, snapshot: Snapshot) -> Self {
//         // Augment the underlying scan to decode the key and filter invisible versions. We don't
//         // return the version, since we don't need it, but beware that all versions of the key
//         // will still be returned - we usually only need the last, which is what the next() and
//         // next_back() methods need to handle. We also don't decode the value, since we only need
//         // to decode the last version.
//         scan = Box::new(scan.filter_map(move |r| {
//             r.and_then(|(k, v)| match Key::decode(&k)? {
//                 Key::Record(_, version) if !snapshot.is_visible(version) => Ok(None),
//                 Key::Record(key, _) => Ok(Some((key.into_owned(), v))),
//                 k => Err(Error::Internal(format!("Expected Record, got {:?}", k))),
//             })
//             .transpose()
//         }));
//         Self { scan: scan.peekable(), next_back_seen: None }
//     }
//
//     // next() with errors handling.
//     fn try_next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
//         while let Some((key, value)) = self.scan.next().transpose()? {
//             // Only return the item if it is the last version of the key.
//             if match self.scan.peek() {
//                 Some(Ok((peek_key, _))) if *peek_key != key => true,
//                 Some(Ok(_)) => false,
//                 Some(Err(err)) => return Err(err.clone()),
//                 None => true,
//             } {
//                 // Only return non-deleted items.
//                 if let Some(value) = deserialize(&value)? {
//                     return Ok(Some((key, value)));
//                 }
//             }
//         }
//         Ok(None)
//     }
//
//     /// next_back() with errors handling.
//     fn try_next_back(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
//         while let Some((key, value)) = self.scan.next_back().transpose()? {
//             // Only return the last version of the key (so skip if seen).
//             if match &self.next_back_seen {
//                 Some(seen_key) if *seen_key != key => true,
//                 Some(_) => false,
//                 None => true,
//             } {
//                 self.next_back_seen = Some(key.clone());
//                 // Only return non-deleted items.
//                 if let Some(value) = deserialize(&value)? {
//                     return Ok(Some((key, value)));
//                 }
//             }
//         }
//         Ok(None)
//     }
// }
