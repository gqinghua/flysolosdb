use crate::storage::kv;

//底层调用kv
pub struct KV {
    ///底层键/值存储
    pub(super) kv: kv::MVCC,
}
