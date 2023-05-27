use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::sql::db::table::Table;

use crate::error::error::{Result, SQLRiteError};

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
    /// let mut db = sql::db::database::Database::new("my_db".to_string());
    pub fn new(db_name: String) -> Self {
        Database {
            db_name,
            tables: HashMap::new(),
        }
    }
   
    /// 如果数据库包含以指定键作为表名的表，则返回true。
    pub fn contains_table(&self, table_name: String) -> bool {
        self.tables.contains_key(&table_name)
    }

        ///如果数据库包含sql::db::table:: table，则返回一个不可变引用' sql::db::table '
        ///使用指定键作为表名的表。
        ///
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
}


// 测试代码
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::create::CreateQuery;
    use sqlparser::dialect::SQLiteDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn new_database_create_test() {
        let db_name = String::from("my_db");
        let db = Database::new(db_name.to_string());
        assert_eq!(db.db_name, db_name);
    }

    #[test]
    fn contains_table_test() {
        let db_name = String::from("my_db");
        let mut db = Database::new(db_name.to_string());

        let query_statement = "CREATE TABLE contacts (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULl,
            email TEXT NOT NULL UNIQUE
        );";
        let dialect = SQLiteDialect {};
        let mut ast = Parser::parse_sql(&dialect, &query_statement).unwrap();
        if ast.len() > 1 {
            panic!("Expected a single query statement, but there are more then 1.")
        }
        let query = ast.pop().unwrap();

        let create_query = CreateQuery::new(&query).unwrap();
        let table_name = &create_query.table_name;
        db.tables
            .insert(table_name.to_string(), Table::new(create_query));

        assert!(db.contains_table("contacts".to_string()));
    }

    #[test]
    fn get_table_test() {
        let db_name = String::from("my_db");
        let mut db = Database::new(db_name.to_string());

        let query_statement = "CREATE TABLE contacts (
            id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULl,
            email TEXT NOT NULL UNIQUE
        );";
        let dialect = SQLiteDialect {};
        let mut ast = Parser::parse_sql(&dialect, &query_statement).unwrap();
        if ast.len() > 1 {
            panic!("Expected a single query statement, but there are more then 1.")
        }
        let query = ast.pop().unwrap();

        let create_query = CreateQuery::new(&query).unwrap();
        let table_name = &create_query.table_name;
        db.tables
            .insert(table_name.to_string(), Table::new(create_query));

        let table = db.get_table(String::from("contacts")).unwrap();
        assert_eq!(table.columns.len(), 4);

        let mut table = db.get_table_mut(String::from("contacts")).unwrap();
        table.last_rowid += 1;
        assert_eq!(table.columns.len(), 4);
        assert_eq!(table.last_rowid, 1);
    }
}
