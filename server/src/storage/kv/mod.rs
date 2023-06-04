
pub mod mvcc;




use  crate::error::error::Result;

use std::fmt::Display;
use std::ops::{Bound, RangeBounds};

/////键/值范围上的迭代器。
pub type Scan = Box<dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send>;


///键/值存储
pub trait Store: Display + Send + Sync {
    ///删除键，如果不存在则不执行任何操作。
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    ///将所有缓存的数据刷新到底层存储介质。
    fn flush(&mut self) -> Result<()>;

    ///获取键的值(如果存在)。
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

   ///在键/值对的有序范围内迭代。
    fn scan(&self, range: Range) -> Scan;

   ///为一个键设置一个值，替换现有的值。
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;
}


//迭代器
pub struct Range {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}
impl Range {
    ///从给定的Rust范围创建一个新的范围。我们不能直接使用RangeBounds
    /// scan()，因为它阻止我们使用Store作为trait对象。而且，我们不能拿
    /// AsRef<[u8]>或其他方便的类型，因为它不能用于例如…范围。
    pub fn from<R: RangeBounds<Vec<u8>>>(range: R) -> Self {
        Self {
            start: match range.start_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            end: match range.end_bound() {
                Bound::Included(v) => Bound::Included(v.to_vec()),
                Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
        }
    }
}
