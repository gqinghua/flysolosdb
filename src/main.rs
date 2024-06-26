pub mod block;
pub mod compact;
pub mod iterators;
pub mod key;
pub mod lsm_storage;
pub mod sstable;
pub mod two_merge_iterator;
pub mod minilsm;
pub mod memtable;
pub mod sql;

use anyhow::{Context, Result};
use bytes::{BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use lsm_storage::{LsmStorageInner, LsmStorageOptions};
use memtable::MemTable;
use sql::*;
use tokenize::parse;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Write};
use std::path::{self, Path};
use std::sync::Arc;

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};
use crate::tokenize::tokenize as tk;
use anyhow::bail;
use bytes::Buf;
use parking_lot::Mutex;
use std::sync::atomic::AtomicUsize;
use tempfile::tempdir;

// }

fn main() {
    tracing_subscriber::registry().with(fmt::layer()).init();
    let dir = tempdir().unwrap();
    // print!("dir111{:?}", dir);
    let path = r"E:\tmp\a";

    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
    // let memtable = MemTable::create(0,dir).unwrap();
    // print!("{}", 111);
    // memtable.put(b"key1", b"value1111").unwrap();
    let data = memtable.for_testing_get_slice(b"key1").unwrap();
    tracing::info!("{:?}", data);

    let dir = tempdir().unwrap();
    tracing::info!("本地文件夹为{:?}", dir);
    let path = r"E:\tmp";

    let mut options = LsmStorageOptions::default_for_week1_test();
    options.target_sst_size = 1024;
    options.num_memtable_limit = 1000;
    // let storage =
    //     Arc::new(LsmStorageInner::open(path, LsmStorageOptions::default_for_week1_test()).unwrap());
        let storage =
        Arc::new(LsmStorageInner::open(path, options).unwrap());
    assert_eq!(&storage.get(b"0").unwrap(), &None);
    tracing::info!("Bytes为{:?}", &storage.get(b"0").unwrap());
    // storage.put(b"1", b"1").unwrap();
    // storage.put(b"2", b"121").unwrap();
    // storage.put(b"3", b"3").unwrap();
    // // storage.put(b"4", b"444444444444").unwrap();
    // // // storage.put(b"5", b"5555555555555555555555555555555555555555555").unwrap();

    // // assert_eq!(&storage.get(b"1").unwrap().unwrap()[..], b"233");
    // tracing::info!("Bb1为数据为{:?}", &storage.get(b"1").unwrap().unwrap());
    // tracing::info!("Bb2为数据为{:?}", &storage.get(b"2").unwrap().unwrap());
    // tracing::info!("Bb3为数据为{:?}", &storage.get(b"3").unwrap().unwrap());
    // tracing::info!("Bb4为数据为{:?}", &storage.get(b"4").unwrap().unwrap());
    // tracing::info!("Bb5为数据为{:?}", &storage.get(b"5").unwrap().unwrap());


    tracing::info!("state_lock数据为{:?}", &storage.state_lock);

    storage
    .force_freeze_memtable(&storage.state_lock.lock())
    .unwrap();
   tracing::info!("读取源数据为{:?}", &storage.state.read().imm_memtables.len());
   let previous_approximate_size = storage.state.read().imm_memtables[0].approximate_size();
   tracing::info!("读取源数据长度1为{:?}", &previous_approximate_size);

   for _ in 0..1000 {
     storage.put(b"1", b"2333").unwrap();
    }
    // tracing::info!("Bb5为数据为{:?}", &storage.get(b"5").unwrap().unwrap());

    let num_imm_memtables = storage.state.read().imm_memtables.len();
    tracing::info!("读取源数据长度2为{:?}", &num_imm_memtables);
    assert!(num_imm_memtables >= 1, "no memtable frozen?");
    tracing::info!("Bb1为数据为{:?}", &storage.get(b"1").unwrap().unwrap());

//    assert!(
//     storage.state.read().imm_memtables[1].approximate_size() == previous_approximate_size,
//     "wrong order of memtables?"
// );


}
#[cfg(test)]
#[test]
fn test001() {
    tracing_subscriber::registry().with(fmt::layer()).init();
    let sql = "SELECT id, name FROM users";
    let tokens = tk(sql);
    tracing::info!("tokens{:?}", tokens);
    let parsed = parse(&tokens);
    tracing::info!("002{:?}", parsed);

    let input = 5;
    let result = some_function(input);
    tracing::info!("The input to some_function was: {}", input);
    tracing::info!("The result of some_function is: {}", result);
}
fn some_function(num: i32) -> i32 {
    num * 2
}