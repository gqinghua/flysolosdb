

use std::sync::Arc;

use bytes::{Buf, BufMut, Bytes};

use crate::key::{KeySlice, KeyVec};


pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

///块是LSM树中最小的读取和缓存单元。它是一个排序的集合
///键值对。
/// //数据和偏移量，向量数组
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}
impl Block {
    fn get_first_key(&self) -> KeyVec {
        let mut buf = &self.data[..];
        buf.get_u16();
        let key_len = buf.get_u16();
        let key = &buf[..key_len as usize];
        KeyVec::from_vec(key.to_vec())
    }
}

pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}


impl Block {
    //数据编码返回字节
    //编码
    pub fn encode(&self) -> Bytes {
        //在Rust中，Vec<u8> 是一个动态数组，其中每个元素都是一个无符号8位整数（即字节，u8）。clone 是一个方法，用于创建该数据结构的深拷贝。
        //当你对一个 Vec<u8> 调用 clone 方法时，你会得到一个与原 Vec<u8> 内容完全相同的新的 Vec<u8> 实例，
        // 但是这两个实例在内存中是分开的，互不干扰。
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // 在块的末尾添加元素的个数
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    //解码 把
    pub fn decode(data: &[u8]) -> Self {
        // 获取块中元素的个数
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        // 获取偏移量数组
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        //检索数据
        let data = data[0..data_end].to_vec();
        Self { data, offsets }
    }
}

/// 迭代一个块。
pub struct BlockIterator {
    ///对块的引用
    block: Arc<Block>,
    /// 迭代器所在位置的当前键
    key: KeyVec,
    /// 块中的当前值范围。数据，对应当前键
    value_range: (usize, usize),
    /// 迭代器所在位置的当前索引
    idx: usize,
    /// 块中的第一个键
    first_key: KeyVec,
}

//实现迭代器
impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: Block::get_first_key(&block),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Seeks to the idx-th key in the block.
    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        self.seek_to_offset(offset);
        self.idx = idx;
    }
    /// Seek to the specified position and update the current `key` and `value`
    /// Index update will be handled by caller
    fn seek_to_offset(&mut self, offset: usize) {
        let mut entry = &self.block.data[offset..];
        // Since `get_u16()` will automatically move the ptr 2 bytes ahead here,
        // we don't need to manually advance it
        let overlap_len = entry.get_u16() as usize;
        let key_len = entry.get_u16() as usize;
        let key = &entry[..key_len];
        self.key.clear();
        self.key.append(&self.first_key.raw_ref()[..overlap_len]);
        self.key.append(key);
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        let value_offset_begin = offset + SIZEOF_U16 + SIZEOF_U16 + key_len + SIZEOF_U16;
        let value_offset_end = value_offset_begin + value_len;
        self.value_range = (value_offset_begin, value_offset_end);
        entry.advance(value_len);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that is >= `key`.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid,
                std::cmp::Ordering::Equal => return,
            }
        }
        self.seek_to(low);
    }
}
