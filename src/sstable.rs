use std::{fs::File, path::Path, sync::Arc};

use crate::{
    block::Block, key::{KeyBytes, KeySlice}, lsm_storage::BlockCache
};
use anyhow::{anyhow, bail, Result};
use bytes::{Buf, BufMut};

//创建sst表数据 ,用于刷新数据到磁盘，要判断数据
pub struct SsTable {
    /// SsTable的实际存储单元，格式如上。
    pub(crate) file: FileObject,
    /// 保存数据块信息的元块。
    pub(crate) block_meta: Vec<BlockMeta>,
    /// 指示' file '中元块起始点的偏移量。
    pub(crate) block_meta_offset: usize,
    //唯一id
    id: usize,
    //缓存的数据
    block_cache: Option<Arc<BlockCache>>,
    //开始数据
    first_key: KeyBytes,
    //介绍key
    last_key: KeyBytes,
    // pub(crate) bloom: Option<Bloom>,
    //ts最大设置
    max_ts: u64,
}
impl SsTable {
    /// 打开sstable文件
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size();
        let raw_bloom_offset = file.read(len - 4, 4)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32() as u64;
        let raw_bloom = file.read(bloom_offset, len - 4 - bloom_offset)?;
        // let bloom_filter = Bloom::decode(&raw_bloom)?;
        let raw_meta_offset = file.read(bloom_offset - 4, 4)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as u64;
        let raw_meta = file.read(block_meta_offset, bloom_offset - 4 - block_meta_offset)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..])?;
        Ok(Self {
            file,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            // bloom: Some(bloom_filter),
            max_ts: 0,
        })
    }
    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
    ///查找可能包含' key '的块。
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        self.block_meta
            .partition_point(|meta| meta.first_key.as_key_slice() <= key)
            .saturating_sub(1)
    }

    /// 用块缓存从磁盘读取一个块。
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(ref block_cache) = self.block_cache {
            let blk = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(blk)
        } else {
            self.read_block(block_idx)
        }
    }
     ///从磁盘读取一个块。
     pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let block_len = offset_end - offset - 4;
        let block_data_with_chksum: Vec<u8> = self
            .file
            .read(offset as u64, (offset_end - offset) as u64)?;
        let block_data = &block_data_with_chksum[..block_len];
        let checksum = (&block_data_with_chksum[block_len..]).get_u32();
        if checksum != crc32fast::hash(block_data) {
            bail!("block checksum mismatched");
        }
        Ok(Arc::new(Block::decode(block_data)))
    }
    
    ///获取数据块的数量。
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }
    
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// 该数据块的偏移量。
    pub offset: usize,
    /// 数据块的第一个键。
    pub first_key: KeyBytes,
    /// 数据块的最后一个键。
    pub last_key: KeyBytes,
}
impl BlockMeta {
    ///将块元编码到缓冲区。
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut estimated_size = std::mem::size_of::<u32>();
        for meta in block_meta {
            // 偏移量的大小
            estimated_size += std::mem::size_of::<u32>();
            //键长度的大小
            estimated_size += std::mem::size_of::<u16>();
            // 实际键的大小
            estimated_size += meta.first_key.len();
            // 键长度的大小
            estimated_size += std::mem::size_of::<u16>();
            // 实际键的大小
            estimated_size += meta.last_key.len();
        }
        estimated_size += std::mem::size_of::<u32>();
        // 预留空间以提高性能，特别是当传入数据的大小为大
        buf.reserve(estimated_size);
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(meta.first_key.raw_ref());
            buf.put_u16(meta.last_key.len() as u16);
            buf.put_slice(meta.last_key.raw_ref());
        }
        buf.put_u32(crc32fast::hash(&buf[original_len + 4..]));
        assert_eq!(estimated_size, buf.len() - original_len);
    }
    /// 从缓冲区解码块元。
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut block_meta = Vec::new();
        let num = buf.get_u32() as usize;
        let checksum = crc32fast::hash(&buf[..buf.remaining() - 4]);
        for _ in 0..num {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buf.copy_to_bytes(first_key_len));
            let last_key_len: usize = buf.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buf.copy_to_bytes(last_key_len));
            block_meta.push(BlockMeta {
                offset,
                first_key,
                last_key,
            });
        }
        if buf.get_u32() != checksum {
            bail!("meta checksum mismatched");
        }

        Ok(block_meta)
    }
}

///一个文件对象。
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        let mut data = vec![0; len as usize];
        // self.0
        //     .as_ref()
        //     .unwrap()
        //     .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}
