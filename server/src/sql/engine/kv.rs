use crate::storage::kv;
use crate::error::error::Result;
//底层调用kv
pub struct KV {
    ///底层键/值存储
    pub(super) kv: kv::MVCC,
}

//修复手动执行克隆，因为https://github.com/rust-lang/rust/issues/26925
impl Clone for KV {
    fn clone(&self) -> Self {
        KV::new(self.kv.clone())
    }
}
//kv实现类
impl KV {
    /// 创建一个新的基于键/值的SQL引擎
    pub fn new(kv: kv::MVCC) -> Self {
        Self { kv }
    }

   ///获取未版本控制的元数据值
    pub fn get_metadata(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.get_metadata(key)
    }

   ///设置未版本控制的元数据值
    pub fn set_metadata(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.set_metadata(key, value)
    }
}

