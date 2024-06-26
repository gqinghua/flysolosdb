use std::{path::Path, sync::Arc};

use parking_lot::Mutex;
use anyhow::{bail, Context, Result};
use crate::lsm_storage::{LsmStorageInner, LsmStorageOptions};

/// ' LsmStorageInner '的包装器和MiniLSM的用户界面。
/// minilsm 在内存中是不是要刷新频繁一点，加大cpu和一级缓存的使用效率
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// 通知L0刷新线程停止工作。
    flush_notifier: crossbeam_channel::Sender<()>,
    /// 刷新线程. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// 通知压缩线程停止工作。
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// 压缩线程的句柄
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

// trait Drop 是一个特殊的trait，用于定义当某个类型的值离开其作用域（即不再被使用）时应该执行的清理操作
// 。这通常用于释放资源，如内存、文件句柄、网络连接等。
// 当一个类型实现了 Drop trait，Rust 的运行时系统会在该值的作用域结束时自动调用该类型的 drop 方法。
// drop 方法并不直接暴露给开发者，而是通过实现 Drop trait 的 drop 函数来定义的。
impl Drop for MiniLsm {
    //停止工作
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

//实现MiniLsm功能
impl MiniLsm {
    //通过加载现有目录或创建新目录启动存储引擎
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }
}
