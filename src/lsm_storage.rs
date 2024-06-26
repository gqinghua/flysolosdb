use std::{
    cmp,
    collections::{BTreeSet, BinaryHeap, HashMap},
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{atomic::AtomicUsize, Arc},
};

use bytes::{Buf, BufMut, Bytes};
use parking_lot::{Mutex, MutexGuard, RwLock};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    block::{Block, BlockIterator, SIZEOF_U16}, compact::{
        CompactionController, LeveledCompactionController, LeveledCompactionOptions,
        SimpleLeveledCompactionController, SimpleLeveledCompactionOptions,
        TieredCompactionController, TieredCompactionOptions,
    }, iterators::{SstConcatIterator, StorageIterator}, key::{KeySlice, KeyVec}, sstable::{FileObject, SsTable}, two_merge_iterator::TwoMergeIterator, MemTable
};

/// LSM树的存储接口。
/// open开启LSM树
pub(crate) struct LsmStorageInner {
    // parking_lot里的RwLock更好用
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    //先不锁定缓存
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    // #[allow(dead_code)]
    // // pub(crate) mvcc: Option<LsmMvccInner>,
    // #[allow(dead_code)]
    // pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}
//清单

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

//源文件
#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    // Compaction(CompactionTask, Vec<usize>),
}
//创建文件
impl Manifest {
    //创建并写入
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create manifest")?,
            )),
        })
    }

    //追加数据
    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = serde_json::to_vec(&record)?;
        let hash = crc32fast::hash(&buf);
        file.write_all(&(buf.len() as u64).to_be_bytes())?;
        buf.put_u32(hash);
        file.write_all(&buf)?;
        file.sync_all()?;
        Ok(())
    }
    //
    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        tracing::info!("recover方法 入参{:?}", path.as_ref());
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover manifest")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut buf_ptr = buf.as_slice();
        let mut records = Vec::new();
        while buf_ptr.has_remaining() {
            let len = buf_ptr.get_u64();
            let slice = &buf_ptr[..len as usize];
            let json = serde_json::from_slice::<ManifestRecord>(slice)?;
            buf_ptr.advance(len as usize);
            let checksum = buf_ptr.get_u32();
            if checksum != crc32fast::hash(slice) {
                bail!("checksum mismatched!");
            }
            records.push(json);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

}
impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
       // 启动存储引擎，要么加载一个现有目录，要么创建一个新目录,目录为数据加载区
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        tracing::info!("options数据为 {:?}", options);
        tracing::info!("path数据为{:?}", path.as_ref());
        //先创建一个资源
        let mut state = LsmStorageState::create(&options);
        let path = path.as_ref();
        let mut next_sst_id = 1;
        // 4GB block cache,
        let block_cache = Arc::new(BlockCache::new(1 << 20)); 
        let manifest;
        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };
        if !path.exists() {
            std::fs::create_dir_all(path).context("failed to create DB dir")?;
        }
        let manifest_path = path.join("MANIFEST");
        tracing::info!("manifest_path 数据为 {:?}", manifest_path);

        if !manifest_path.exists() {
            tracing::info!("test001");
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            tracing::info!("test0011,{:?}", manifest_path);
            manifest = Manifest::create(&manifest_path).context("failed to create manifest")?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            tracing::info!("test002 manifest数据为");
        } else {
            let (m, records) = Manifest::recover(&manifest_path)?;
            tracing::info!("recover返回的数据为{:?}", m.file);

            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(sst_id) => {
                        let res = memtables.remove(&sst_id);
                        assert!(res, "memtable not exist?");
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::NewMemtable(x) => {
                        next_sst_id = next_sst_id.max(x);
                        memtables.insert(x);
                    } //     ManifestRecord::Compaction(task, output) => {
                      //         let (new_state, _) =
                      //             compaction_controller.apply_compaction_result(&state, &task, &output);
                      //         // TODO: apply remove again
                      //         state = new_state;
                      //         next_sst_id =
                      //             next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                      //     }
                }
            }

            let mut sst_cnt = 0;
            // recover SSTs
            for table_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(_, files)| files))
            {
                let table_id = *table_id;
                let sst = SsTable::open(
                    table_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, table_id))
                        .with_context(|| format!("failed to open SST: {}", table_id))?,
                )?;
                state.sstables.insert(table_id, Arc::new(sst));
                sst_cnt += 1;
            }
            println!("{} SSTs opened", sst_cnt);

            next_sst_id += 1;

            // recover memtables
            if options.enable_wal {
                let mut wal_cnt = 0;
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_cnt += 1;
                    }
                }
                println!("{} WALs recovered", wal_cnt);
                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
            next_sst_id += 1;
            manifest = m;
        };
        tracing::info!("test003 manifest数据为");
        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            //定义等级
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            // mvcc: None,
            // compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };
        tracing::info!("test004 storage数据为{:?}", storage.path);
        // storage.sync_dir()?;
        tracing::info!("test004 manifest数据为");

        Ok(storage)
    }
    /// 通过写入当前memtable，将键值对放入存储器。
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

      ///通过写入空值从存储中删除键。
       pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }
    //批量写入接口，循环
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        for record in batch {
            match record {
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty(), "key cannot be empty");
                    let size;
                    {
                        let guard = self.state.read();
                        guard.memtable.put(key, b"")?;
                        size = guard.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Put(key, value) => {
                    tracing::info!("key为数据为{:?}", key.as_ref());
                    tracing::info!("value为数据为{:?}", value.as_ref());
                    let key = key.as_ref();
                    let value = value.as_ref();
                    let size;
                    {
                        let guard = self.state.read();
                        // tracing::info!("guard数据为{:?}", guard);
                        guard.memtable.put(key, value)?;
                        size = guard.memtable.approximate_size();
                    }
                    tracing::info!("size数据为{:?}", &size);
                    self.try_freeze(size)?;
                }
            }
        }
        Ok(())
    }
        //持久化操作
    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        tracing::info!("sestimated_size数据为{:?}", estimated_size);
        tracing::info!("本源数据为{:?}", self.options.target_sst_size );
        if estimated_size >= 1{
            tracing::info!("需要持久化的数据byte为{:?}", self.state_lock.lock());
            tracing::info!("需要持久化的数据路径为{:?}", self.path);
            let state_lock = self.state_lock.lock();
            let guard = self.state.read();
            // the memtable could have already been frozen, check again to ensure we really need to freeze
            if guard.memtable.approximate_size() >= 1{
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }
    //刷新源库数据
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        tracing::info!("刷新源库数据数据为{:?}", &self.state_lock);
        let memtable_id = self.next_sst_id();
        tracing::info!("刷新源库数据数memtable_id为{:?}", memtable_id);
        tracing::info!("刷新源库数据数options为{:?}", self.options.enable_wal);
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            tracing::info!("进入003");
            Arc::new(MemTable::create(memtable_id))
        };
        let clone_data = memtable.clone();

        // tracing::info!("memtable wei{:?}",*clone_data);
        self.freeze_memtable_with_memtable(memtable)?;
        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;
        // self.sync_dir()?;
        Ok(())
    }
    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }
    //刷新写入
    fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let mut guard = self.state.write();
        // 用新的memtable替换当前的memtable。
        let mut snapshot = guard.as_ref().clone();
        tracing::info!("进入005");
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        //将memtable添加到不可变memtable中。
        snapshot.imm_memtables.insert(0, old_memtable.clone());
        //更新快照。
        *guard = Arc::new(snapshot);

        drop(guard);
        old_memtable.sync_wal()?;

        Ok(())
    }

    ///获取元数据
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; 

        //查找当前内存表。
        if let Some(value) = snapshot.memtable.get(key) {
            tracing::info!("4444441");
            if value.is_empty() {
                 //发现，返回键不存在
                return Ok(None);
            }
            tracing::info!("4444442");
            return Ok(Some(value));
        }

        // 在不可变的记忆表上搜索。
        for memtable in snapshot.imm_memtables.iter() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    //发现，返回键不存在
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());

        let keep_table = |key: &[u8], table: &SsTable| {
            if key_within(
                key,
                table.first_key().as_key_slice(),
                table.last_key().as_key_slice(),
            ) {
                // if let Some(bloom) = &table.bloom {
                //     if bloom.may_contain(farmhash::fingerprint32(key)) {
                //         return true;
                //     }
                // } else {
                return true;
                // }
            }
            false
        };

        for table in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table].clone();
            if keep_table(key, &table) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    table,
                    KeySlice::from_slice(key),
                )?));
            }
        }
        let l0_iter = MergeIterator::create(l0_iters);
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in &snapshot.levels {
            let mut level_ssts = Vec::with_capacity(level_sst_ids.len());
            for table in level_sst_ids {
                let table = snapshot.sstables[table].clone();
                if keep_table(key, &table) {
                    level_ssts.push(table);
                }
            }
            let level_iter =
                SstConcatIterator::create_and_seek_to_key(level_ssts, KeySlice::from_slice(key))?;
            level_iters.push(Box::new(level_iter));
        }

        // let iter = TwoMergeIterator::create(l0_iter, MergeIterator::create(level_iters))?;

        // if iter.is_valid() && iter.key().raw_ref() == key && !iter.value().is_empty() {
        //     return Ok(Some(Bytes::copy_from_slice(iter.value())));
        // }
        tracing::info!("444444");
        Ok(None)
    }
    pub(super) fn sync_dir(&self) -> Result<()> {
        tracing::info!("test003 manifest path 数据为{:?}", self.path);
        let text: &str = "a.txt";
        File::open(&self.path)?.sync_all()?;
        // File::open(text).unwrap();
        Ok(())
    }
    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }
    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }
}
fn key_within(user_key: &[u8], table_begin: KeySlice, table_end: KeySlice) -> bool {
    table_begin.raw_ref() <= user_key && user_key <= table_end.raw_ref()
}

/// 遍历SSTable对象内容的迭代器。
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}
impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
            }
        }
        Ok(())
    }
}
///合并多个相同类型的迭代器。如果相同的键多次出现
///迭代器，首选索引较小的迭代器。
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        let mut heap = BinaryHeap::new();

        if iters.iter().all(|x| !x.is_valid()) {
            // All invalid, select the last one as the current.
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}


impl SsTableIterator {
    /// 创建一个新的迭代器并查找>= ' key '的第一个键值对。
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (blk_idx, blk_iter) = Self::seek_to_key_inner(&table, key)?;
        let iter = Self {
            blk_iter,
            table,
            blk_idx,
        };
        Ok(iter)
    }
    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut blk_idx = table.find_block_idx(key);
        let mut blk_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(blk_idx)?, key);
        if !blk_iter.is_valid() {
            blk_idx += 1;
            if blk_idx < table.num_of_blocks() {
                blk_iter =
                    BlockIterator::create_and_seek_to_first(table.read_block_cached(blk_idx)?);
            }
        }
        Ok((blk_idx, blk_iter))
    }
}
//数据类型，是put还是删除Del
pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}


///表示存储引擎的状态。
#[derive(Clone)]
pub struct LsmStorageState {
    /// 当前记忆表。
    pub memtable: Arc<MemTable>,
    /// 不可变的记忆表，从最新到最早。
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, 从最新到最早。
    pub l0_sstables: Vec<usize>,
    /// 按键范围排序的sstable;L1 - L_max用于分层压缩，或分层使用tiers
    /// 压实.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

//实现 lsm结构数据
impl LsmStorageState {
    //当创建时会初始化长度
    fn create(options: &LsmStorageOptions) -> Self {
        //长度等级是符合Leveled和Simple时创建的长度不同
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            //动态数据 ，数组
            CompactionOptions::Tiered(_) => Vec::new(),
            //动态数组
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        //返回selef对象
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}
//LSM树的存储接口。
#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // 以字节为单位的块大小
    pub block_size: usize,
    // 以字节为单位的SST大小，也是memtable容量的近似限制
    pub target_sst_size: usize,
    // 内存中内存表的最大数目，超过此限制时刷新到L0
    pub num_memtable_limit: usize,
    //压缩等级
    pub compaction_options: CompactionOptions,
    //
    pub enable_wal: bool,
    //是否序列化
    pub serializable: bool,
}

//实现LsmStorageOptions
impl LsmStorageOptions {
    //返回一个对象
    pub fn default_for_week1_test() -> Self {
        //默认时NoCompaction，
        Self {
            block_size: 4096,
            //二进制左移是为什么？
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            //是否启用wal
            enable_wal: false,
            //
            num_memtable_limit: 50,
            //不序列化
            serializable: false,
        }
    }
}

//数据在内存中的压缩等级
#[derive(Debug, Clone)]
pub enum CompactionOptions {
    //水平压实与部分压实+动态水平支持(= RocksDB的水平 压实)
    Leveled(LeveledCompactionOptions),
    ///分层压实(= RocksDB的通用压实)
    Tiered(TieredCompactionOptions),
    /// 简单平整压实
    Simple(SimpleLeveledCompactionOptions),
    /// 在无压缩模式下(第1周)，总是刷新到L0
    NoCompaction,
}

