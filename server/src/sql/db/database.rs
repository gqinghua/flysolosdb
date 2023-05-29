use serde::{Deserialize, Serialize};
use sqlparser::ast::Statement;
use std::collections::HashMap;
use std::fs;
use std::ptr::null;

use crate::error::database::DatabaseError;
use crate::error::error::{Result, SQLRiteError};
use crate::sql::db::table::Table;

use std::{
    io::Write,
    path::{Path, PathBuf},
};

pub const DB_DIR: &str = "./data";
pub const CURR_DB: &str = "curr_db";

// 数据库名称和暂时存储数据的地方
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Database {
    ///数据库的名称。(模式名，而不是文件名)
    pub db_name: String,
    ///此数据库中表的HashMap
    pub tables: HashMap<String, Table>,
}

//数据库实现
impl Database {
    // 创建一个空的数据库
    pub fn new(db_name: &str) -> Self {
        //修改为本地存储
        let base_dir = std::path::Path::new(DB_DIR);
        let db_dir = base_dir.join(db_name);
        print!("数据库名称{}", db_name);
        if db_dir.exists() {
            print!("错误")
        }
        fs::create_dir_all(db_dir).unwrap();
        Database {
            db_name: db_name.to_string(),
            tables: HashMap::new(),
        }
    }

    /// 如果数据库包含以指定键作为表名的表，则返回true。
    pub fn contains_table(&self, table_name: String) -> bool {
        self.tables.contains_key(&table_name)
    }

    ///如果数据库包含sql::db::table:: table，则返回一个不可变引用' sql::db::table '
    ///使用指定键作为表名的表。
    pub fn get_table(&self, table_name: String) -> Result<&Table> {
        if let Some(table) = self.tables.get(&table_name) {
            Ok(table)
        } else {
            Err(SQLRiteError::General(String::from("Table not found.")))
        }
    }

    ///如果数据库中包含sql::db::table:: table，则返回一个可变引用
    ///使用指定键作为表名的表。
    pub fn get_table_mut(&mut self, table_name: String) -> Result<&mut Table> {
        if let Some(table) = self.tables.get_mut(&table_name) {
            Ok(table)
        } else {
            Err(SQLRiteError::General(String::from("Table not found.")))
        }
    }
    //指定表
    pub fn use_db(statement: &Statement) ->  Result<()> {
        match statement {
            Statement::Use { db_name } => {
                let ident = db_name;
                let NameValue = &ident.value;
                print!("aaa111  {}",NameValue);
                let base_dir = std::path::Path::new(DB_DIR);
                Database::exists_or_err(&NameValue)?;
                let curr_db = base_dir.join(CURR_DB);
                fs::write(curr_db, NameValue).unwrap();
            }
            _ => unimplemented!(),
        }
        Ok(())
    }
    //是否存在
    pub fn exists_or_err(name: &str) -> Result<()> {
        if !Database::exists(name) {
            Err(SQLRiteError::General(String::from("Table not found.")))
        } else {
            Ok(())
        }
    }
    //判断
    pub fn exists(name: &str) -> bool {
        let path = Self::get_db_path(name);
        path.exists()
    }
    //查询路径
    pub fn get_db_path(name: &str) -> PathBuf {
        let base_dir = std::path::Path::new(DB_DIR);
        let db_dir = base_dir.join(name);
        db_dir
    }
}
