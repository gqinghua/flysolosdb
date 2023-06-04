use std::sync::RwLock;

use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLockReadGuard, RwLockWriteGuard};

use super::Store;

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
