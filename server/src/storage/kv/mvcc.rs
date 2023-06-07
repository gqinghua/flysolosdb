use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};

use super::Store;
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



/// An MVCC transaction.
pub struct Transaction {
    /// The underlying store for the transaction. Shared between transactions using a mutex.
    store: Arc<RwLock<Box<dyn Store>>>,
    /// The unique transaction ID.
    id: u64,
    /// The transaction mode.
    mode: Mode,
    /// The snapshot that the transaction is running in.
    snapshot: Snapshot,
}