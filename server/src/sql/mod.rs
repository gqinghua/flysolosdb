pub mod db;
pub mod parser;
pub mod regrxs;

use crate::error::error::{Result, SQLRiteError};
use crate::sql::db::database::Database;
use crate::sql::db::table::Table;

use parser::create::CreateQuery;
use parser::insert::InsertQuery;
use parser::query::Select;
use parser::query::SelectQuery;
use prettytable::{Cell as PrintCell, Row as PrintRow, Table as PrintTable};
use sqlparser::ast::Statement;
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::{Parser, ParserError};

//sql类型 枚举值
#[derive(Debug, PartialEq)]
pub enum SQLCommand {
    Insert(String),
    Delete(String),
    Update(String),
    CreateTable(String),
    Select(String),
    USE(String),
    DROP(String),
    Unknown(String),
}
// 枚举值实现。new 创建一个对象
impl SQLCommand {
    pub fn new(command: String) -> SQLCommand {
        let v = command.split(" ").collect::<Vec<&str>>();
        match v[0] {
            "insert" => SQLCommand::Insert(command),
            "update" => SQLCommand::Update(command),
            "delete" => SQLCommand::Delete(command),
            "create" => SQLCommand::CreateTable(command),
            "select" => SQLCommand::Select(command),
            _ => SQLCommand::Unknown(command),
        }
    }
}

//调用数据
pub fn process_command(query: &str, db: &mut Database) -> Result<String> {
    let dialect = SQLiteDialect {};
    let message: String;
    let mut ast = Parser::parse_sql(&dialect, &query).map_err(SQLRiteError::from)?;

    if ast.len() > 1 {
        return Err(SQLRiteError::SqlError(ParserError::ParserError(format!(
            "Expected a single query statement, but there are {}",
            ast.len()
        ))));
    }

    let query = ast.pop().unwrap();

    //最初只实现一些基本的SQL语句
    match query {
        // 创建
        Statement::CreateTable { .. } => {
            let create_query = CreateQuery::new(&query);
            match create_query {
                Ok(payload) => {
                    let table_name = payload.table_name.clone();

                    println!(
                        "数据库是否存在{}",
                        &db.contains_table(table_name.to_string())
                    );

                    // 在解析CREATE table查询后，检查表是否已经存在
                    match db.contains_table(table_name.to_string()) {
                        true => {
                            return Err(SQLRiteError::Internal(
                                "Cannot create, table already exists.".to_string(),
                            ));
                        }
                        false => {
                            let table = Table::new(payload);
                            //打印格式化和对齐的表格打印机
                            let _ = table.print_table_schema();
                            //
                            db.tables.insert(table_name.to_string(), table);
                            //遍历所有内容
                            // for (table_name, _) in &db.tables {
                            //     println!("{}" , table_name);
                            // }
                            //执行的语句。
                            message = String::from("CREATE TABLE Statement executed.");
                        }
                    }
                }
                Err(err) => return Err(err),
            }
        }
        // 分割插入
        Statement::Insert { .. } => {
            let insert_query = InsertQuery::new(&query);
            match insert_query {
                Ok(payload) => {
                    let table_name = payload.table_name;
                    let columns = payload.columns;
                    let values = payload.rows;

                    // 检查数据库中是否存在表
                    println!(
                        "查询表明是否存在{:?}",
                        db.contains_table(table_name.to_string())
                    );
                    match db.contains_table(table_name.to_string()) {
                        true => {
                            let db_table = db.get_table_mut(table_name.to_string()).unwrap();

                            // Checking if columns on INSERT query exist on Table
                            match columns
                                .iter()
                                .all(|column| db_table.contains_column(column.to_string()))
                            {
                                true => {
                                    println!("判断查询表名称1");
                                    for value in &values {
                                        // Checking if number of columns in query are the same as number of values
                                        if columns.len() != value.len() {
                                            return Err(SQLRiteError::Internal(format!(
                                                "{} values for {} columns",
                                                value.len(),
                                                columns.len()
                                            )));
                                        }
                                        match db_table.validate_unique_constraint(&columns, value) {
                                            Ok(()) => {
                                                //  没有唯一约束违反，继续插入行
                                                db_table.insert_row(&columns, &value);
                                            }
                                            Err(err) => {
                                                return Err(SQLRiteError::Internal(format!(
                                                    "Unique key constaint violation: {}",
                                                    err
                                                )))
                                            }
                                        }
                                    }
                                }
                                false => {
                                    return Err(SQLRiteError::Internal(
                                        "Cannot insert, some of the columns do not exist"
                                            .to_string(),
                                    ));
                                }
                            }
                            db_table.print_table_data();
                        }
                        false => {
                            //发生了一个错误:内部错误:表不存在
                            return Err(SQLRiteError::Internal("Table doesn't exist".to_string()));
                        }
                    }
                }
                Err(err) => return Err(err),
            }
            //INSERT已执行的语句。
            message = String::from("INSERT Statement executed.")
        }
        //打印查询sql语句
        Statement::Query(..) => {
            let select_query = SelectQuery::new(&query);
            match select_query {
                Ok(mut sq) => match db.contains_table(sq.from.to_string()) {
                    true => {
                        let db_table = db.get_table(sq.from.to_string()).unwrap();

                        let cloned_projection = sq.projection.clone();

                        for p in &cloned_projection {
                            if p == "*" {
                                let new_projections = db_table
                                    .columns
                                    .iter()
                                    .map(|c| c.column_name.to_string())
                                    .collect::<Vec<String>>();
                                sq.insert_projections(new_projections);
                            }
                        }

                        for col in &sq.projection {
                            if !db_table.contains_column((&col).to_string()) {
                                println!(
                                    "Cannot execute query, cannot find column {} in table {}",
                                    col, db_table.tb_name
                                );
                            }
                        }
                        println!("sq = {:?}", &sq);
                        db_table.execute_select_query(&sq);
                    }
                    false => {
                        eprintln!("Cannot execute query the table {} doesn't exists", sq.from)
                    }
                },
                Err(error) => eprintln!("{error}"),
            }
            //QUERY已执行的语句。
            message = String::from("QUERY Statement executed.")
        }
        //Statement::Query(_query) => message = String::from("SELECT Statement executed."),
        Statement::Use { .. } => {
            let use_query = Database::use_db(&query);
            message = String::from("INSERT111 Statement executed.")
        }
        Statement::CreateDatabase { .. } => {
            let use_query = Database::new("aaa");
            message = String::from("INSERT111 Statement executed.")

        }

        Statement::Delete { .. } => message = String::from("DELETE Statement executed."),
        _ => {
            return Err(SQLRiteError::NotImplemented(
                "SQL Statement not supported yet.".to_string(),
            ))
        }
    };

    Ok(message)
}
