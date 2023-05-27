use sqlparser::ast::{ColumnOption, DataType, Statement};

use crate::error::error::{Result, SQLRiteError};
use serde::{Deserialize, Serialize};

///每个表中每个SQL列的模式由
///下面的结构经过解析和标记
#[derive(PartialEq, Debug)]
pub struct ParsedColumn {
    ///列的名称
    pub name: String,
    /// String格式列的数据类型
    pub datatype: String,
    ////如果列是主键
    pub is_pk: bool,
    ///表示if列声明为NOT NULL约束的值
    pub not_null: bool,
    ///表示if列声明时使用UNIQUE约束的值
    pub is_unique: bool,
}

///下面的结构表示一个已经解析的CREATE TABLE查询
///和分解为name和' ParsedColumn '元数据的Vector
#[derive(Debug)]
pub struct CreateQuery {
    ///查询结束后的表名
    pub table_name: String,
    ///包含列元数据信息的ParsedColumn类型的向量
    pub columns: Vec<ParsedColumn>,
}

impl CreateQuery {
    pub fn new(statement: &Statement) -> Result<CreateQuery> {
        match statement {
            //确认语句为sqlparser::ast:Statement::CreateTable
            Statement::CreateTable {
                name,
                columns,
                constraints: _constraints,
                with_options: _with_options,
                external: _external,
                file_format: _file_format,
                location: _location,
                ..
            } => {
                let table_name = name;
                let mut parsed_columns: Vec<ParsedColumn> = vec![];

                //遍历从Parser::parse:sql返回的列
                //在mod SQL中
                for col in columns {
                    let name = col.name.to_string();

                    //检查列是否已添加到parsed_columns，如果已添加，则返回错误
                    if parsed_columns.iter().any(|col| col.name == name) {
                        return Err(SQLRiteError::Internal(format!(
                            "Duplicate column name: {}",
                            &name
                        )));
                    }


                    //解析每个列的数据类型
                    //目前只接受基本数据类型
                    let datatype = match &col.data_type {
                        DataType::SmallInt(_) => "Integer",
                        DataType::Int(_) => "Integer",
                        DataType::BigInt(_) => "Integer",
                        DataType::Boolean => "Bool",
                        DataType::Text => "Text",
                        DataType::Varchar(_bytes) => "Text",
                        DataType::Real => "Real",
                        DataType::Float(_precision) => "Real",
                        DataType::Double => "Real",
                        // DataType::Decimal(_) => "Real",
                        _ => {
                            eprintln!("not matched on custom type");
                            "Invalid"
                        }
                    };
                    //检查列是否为主键
                    let mut is_pk: bool = false;
                    //检查列是否唯一
                    let mut is_unique: bool = false;
                    //检查列是否为null
                    let mut not_null: bool = false;
                    for column_option in &col.options {
                        match column_option.option {
                            ColumnOption::Unique { is_primary } => {
                                //目前，只有Integer和Text类型可以是primary KEY和Unique
                                //因此被索引。
                                if datatype != "Real" && datatype != "Bool" {
                                    is_pk = is_primary;
                                    if is_primary {
                                        // Checks if table being created already has a PRIMARY KEY, if so, returns an error
                                        if parsed_columns.iter().any(|col| col.is_pk == true) {
                                            return Err(SQLRiteError::Internal(format!(
                                                "Table '{}' has more than one primary key",
                                                &table_name
                                            )));
                                        }
                                        not_null = true;
                                    }
                                    is_unique = true;
                                }
                            }
                            ColumnOption::NotNull => {
                                not_null = true;
                            }
                            _ => (),
                        };
                    }

                    parsed_columns.push(ParsedColumn {
                        name,
                        datatype: datatype.to_string(),
                        is_pk,
                        not_null,
                        is_unique,
                    });
                }
                

                // TODO:处理约束;
                //默认值等。
                for constraint in _constraints {
                    println!("{:?}", constraint);
                }

                let b = CreateQuery {
                    table_name: table_name.to_string(),
                    columns: parsed_columns,
                };
                return Ok((b));
            }

            _ => return Err(SQLRiteError::Internal("Error parsing query".to_string())),
        }
    }
}
