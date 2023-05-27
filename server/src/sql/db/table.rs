use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::rc::Rc;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use prettytable::{Cell as PrintCell, Row as PrintRow, Table as PrintTable};

use crate::error::error::{Result, SQLRiteError};
use crate::sql::parser::{create::CreateQuery};
use crate::sql::parser::query::{SelectQuery,Operator,Binary,Expression};
use std::result::Result as R;
//表信息
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Table {
    /// 表明
    pub tb_name: String,
    ///HashMap，包含关于每列的信息
    pub columns: Vec<Column>,
    /// HashMap，包含关于每行的信息
    pub rows: Rc<RefCell<HashMap<String, Row>>>,
    ///该表上SQL索引的HashMap。
    pub indexes: HashMap<String, String>,
    /// 最近插入的ROWID
    pub last_rowid: i64,
    ///列的名称，如果表没有PRIMARY KEY，则为-1
    pub primary_key: String,
}

impl Table {
    pub fn new(create_query: CreateQuery) -> Self {
        let table_name = create_query.table_name;
        let mut primary_key: String = String::from("-1");
        let columns = create_query.columns;

        let mut table_cols: Vec<Column> = vec![];
        let table_rows: Rc<RefCell<HashMap<String, Row>>> = Rc::new(RefCell::new(HashMap::new()));
        for col in &columns {
            let col_name = &col.name;
            if col.is_pk {
                primary_key = col_name.to_string();
            }
            table_cols.push(Column::new(
                col_name.to_string(),
                col.datatype.to_string(),
                col.is_pk,
                col.not_null,
                col.is_unique,
            ));

            match DataType::new(col.datatype.to_string()) {
                DataType::Integer => table_rows
                    .clone()
                    .borrow_mut()
                    .insert(col.name.to_string(), Row::Integer(BTreeMap::new())),
                DataType::Real => table_rows
                    .clone()
                    .borrow_mut()
                    .insert(col.name.to_string(), Row::Real(BTreeMap::new())),
                DataType::Text => table_rows
                    .clone()
                    .borrow_mut()
                    .insert(col.name.to_string(), Row::Text(BTreeMap::new())),
                DataType::Bool => table_rows
                    .clone()
                    .borrow_mut()
                    .insert(col.name.to_string(), Row::Bool(BTreeMap::new())),
                DataType::Invalid => table_rows
                    .clone()
                    .borrow_mut()
                    .insert(col.name.to_string(), Row::None),
                DataType::None => table_rows
                    .clone()
                    .borrow_mut()
                    .insert(col.name.to_string(), Row::None),
            };
        }

        Table {
            tb_name: table_name,
            columns: table_cols,
            rows: table_rows,
            indexes: HashMap::new(),
            last_rowid: 0,
            primary_key: primary_key,
        }
    }

    ///返回一个' bool '，通知是否存在具有特定名称的' Column '
    pub fn contains_column(&self, column: String) -> bool {
        self.columns.iter().any(|col| col.column_name == column)
    }

        ///返回' sql::db::table::Column '的不可变引用
        ///用指定的键作为列名的列。
    pub fn get_column(&mut self, column_name: String) -> Result<&Column> {
        if let Some(column) = self
            .columns
            .iter()
            .filter(|c| c.column_name == column_name)
            .collect::<Vec<&Column>>()
            .first()
        {
            Ok(column)
        } else {
            Err(SQLRiteError::General(String::from("Column not found.")))
        }
    }

  

        ///返回' sql::db::table::Column '的可变引用
        ///用指定的键作为列名的列。
    pub fn get_column_mut<'a>(&mut self, column_name: String) -> Result<&mut Column> {
        for elem in self.columns.iter_mut() {
            if elem.column_name == column_name {
                return Ok(elem);
            }
        }
        Err(SQLRiteError::General(String::from("Column not found.")))
    }

    /// Validates if columns and values being inserted violate the UNIQUE constraint
    /// As a reminder the PRIMARY KEY column automatically also is a UNIQUE column.
    ///
    pub fn validate_unique_constraint(
        &mut self,
        cols: &Vec<String>,
        values: &Vec<String>,
    ) -> Result<()> {
        for (idx, name) in cols.iter().enumerate() {
            let column = self.get_column_mut(name.to_string()).unwrap();
            // println!(
            //     "name: {} | is_pk: {} | is_unique: {}, not_null: {}",
            //     name, column.is_pk, column.is_unique, column.not_null
            // );
            if column.is_unique {
                let col_idx = &column.index;
                if *name == *column.column_name {
                    let val = &values[idx];
                    match col_idx {
                        ColumnIndex::Integer(index) => {
                            if index.contains_key(&val.parse::<i32>().unwrap()) {
                                return Err(SQLRiteError::General(format!(
                                    "Error: unique constraint violation for column {}.
                        Value {} already exists for column {}",
                                    *name, val, *name
                                )));
                            }
                        }
                        ColumnIndex::Text(index) => {
                            if index.contains_key(val) {
                                return Err(SQLRiteError::General(format!(
                                    "Error: unique constraint violation for column {}.
                        Value {} already exists for column {}",
                                    *name, val, *name
                                )));
                            }
                        }
                        ColumnIndex::None => {
                            return Err(SQLRiteError::General(format!(
                                "Error: cannot find index for column {}",
                                name
                            )));
                        }
                    };
                }
            }
        }
        return Ok(());
    }

        ///在相应的列中插入所有的VALUES，使用ROWID在所有的行上嵌入INDEX
        ///每个' Table '都会跟踪' last_rowid '，以便于下一个将是什么。
        ///这种数据结构的一个限制是，我们一次只能有一个写事务，否则
        ///我们可以在last_rovid .println!
        ///
        ///由于我们松散地模仿SQLite，这也是SQLite的一个限制(一次只允许一个写事务)
        ///所以我们很好。:)
        ///
    pub fn insert_row(&mut self, cols: &Vec<String>, values: &Vec<String>) {
        let mut next_rowid = self.last_rowid + i64::from(1);
        // //检查表是否有PRIMARY KEY
        if self.primary_key != "-1" {
          //检查主键是否在INSERT QUERY列中
          //如果不是，给它分配next_rowid
            if !cols.iter().any(|col| col == &self.primary_key) {
                let rows_clone = Rc::clone(&self.rows);
                let mut row_data = rows_clone.as_ref().borrow_mut();
                let mut table_col_data = row_data.get_mut(&self.primary_key).unwrap();

                // 根据列名获取标题
                let column_headers = self.get_column_mut(self.primary_key.to_string()).unwrap();

                // 获取列的索引(如果存在)
                let col_index = column_headers.get_mut_index();

                // 我们只在ROW是PRIMARY KEY和INTEGER类型的情况下自动分配
                match &mut table_col_data {
                    Row::Integer(tree) => {
                        let val = next_rowid as i32;
                        tree.insert(next_rowid.clone(), val);
                        if let ColumnIndex::Integer(index) = col_index {
                            index.insert(val, next_rowid.clone());
                        }
                    }
                    _ => (),
                }
            } else {
                //如果主键列在INSERT查询的列列表中，
                //我们在查询的VALUES部分获取分配给它的值
                //并将其赋值给next_rowid，这样每个值都被相同的rowid索引
                //另外，下一个ROWID应该保持自上一个ROWID的自动递增
                let rows_clone = Rc::clone(&self.rows);
                let mut row_data = rows_clone.as_ref().borrow_mut();
                let mut table_col_data = row_data.get_mut(&self.primary_key).unwrap();

                // Again, this is only valid for PRIMARY KEYs of INTEGER type
                match &mut table_col_data {
                    Row::Integer(_) => {
                        for i in 0..cols.len() {
                            // Getting column name
                            let key = &cols[i];
                            if key == &self.primary_key {
                                let val = &values[i];
                                next_rowid = val.parse::<i64>().unwrap();
                            }
                        }
                    }
                    _ => (),
                }
            }
        }

        //这个块检查表中是否缺少任何列
        //执行INSERT语句。如果有，我们将“Null”添加到列中。
        //这样做是因为否则每个值的ROWID引用将是错误的
        //由于行并不总是具有相同的长度。
        let column_names = self
            .columns
            .iter()
            .map(|col| col.column_name.to_string())
            .collect::<Vec<String>>();
        let mut j: usize = 0;
        // For every column in the INSERT statement
        for i in 0..column_names.len() {
            let mut val = String::from("Null");
            let key = &column_names[i];

            if let Some(key) = &cols.get(j) {
                if &key.to_string() == &column_names[i] {
                    // Getting column name
                    val = values[j].to_string();
                    j += 1;
                } else {
                    if &self.primary_key == &column_names[i] {
                        continue;
                    }
                }
            } else {
                if &self.primary_key == &column_names[i] {
                    continue;
                }
            }

            // Getting the rows from the column name
            let rows_clone = Rc::clone(&self.rows);
            let mut row_data = rows_clone.as_ref().borrow_mut();
            let mut table_col_data = row_data.get_mut(key).unwrap();

            // Getting the header based on the column name
            let column_headers = self.get_column_mut(key.to_string()).unwrap();

            // Getting index for column, if it exist
            let col_index = column_headers.get_mut_index();

            match &mut table_col_data {
                Row::Integer(tree) => {
                    let val = val.parse::<i32>().unwrap();
                    tree.insert(next_rowid.clone(), val);
                    if let ColumnIndex::Integer(index) = col_index {
                        index.insert(val, next_rowid.clone());
                    }
                }
                Row::Text(tree) => {
                    tree.insert(next_rowid.clone(), val.to_string());
                    if let ColumnIndex::Text(index) = col_index {
                        index.insert(val.to_string(), next_rowid.clone());
                    }
                }
                Row::Real(tree) => {
                    let val = val.parse::<f32>().unwrap();
                    tree.insert(next_rowid.clone(), val);
                }
                Row::Bool(tree) => {
                    let val = val.parse::<bool>().unwrap();
                    tree.insert(next_rowid.clone(), val);
                }
                Row::None => panic!("None data Found"),
            }
        }
        self.last_rowid = next_rowid;
    }

    ///以一种非常格式化的方式将表模式打印到标准输出
    /// ```
    /// let table = Table::new(payload);
    /// table.print_table_schema();
    ///
    /// Prints to standard output:
    ///    +-------------+-----------+-------------+--------+----------+
    ///    | Column Name | Data Type | PRIMARY KEY | UNIQUE | NOT NULL |
    ///    +-------------+-----------+-------------+--------+----------+
    ///    | id          | Integer   | true        | true   | true     |
    ///    +-------------+-----------+-------------+--------+----------+
    ///    | name        | Text      | false       | true   | false    |
    ///    +-------------+-----------+-------------+--------+----------+
    ///    | email       | Text      | false       | false  | false    |
    ///    +-------------+-----------+-------------+--------+----------+
    /// ```
    ///
    pub fn print_table_schema(&self) -> Result<usize> {

        let mut table = PrintTable::new();

        table.add_row(row![
            "Column Name",
            "Data Type",
            "PRIMARY KEY",
            "UNIQUE",
            "NOT NULL"
        ]);
        for col in &self.columns {
            table.add_row(row![
                col.column_name.to_string(),
                col.datatype.to_string(),
                col.is_pk.to_string(),
                col.is_unique.to_string(),
                col.not_null.to_string(),
            ]);
        }
        
        let lines = table.printstd();
        Ok(44)
    }

    /// P一种非常格式化的方式将表模式打印到标准输出
    ///
    /// # Example
    ///
    ///
    ///     +----+---------+------------------------+
    ///     | id | name    | email                  |
    ///     +----+---------+------------------------+
    ///     | 1  | "Jack"  | "jack@mail.com"        |
    ///     +----+---------+------------------------+
    ///     | 10 | "Bob"   | "bob@main.com"         |
    ///     +----+---------+------------------------+
    ///     | 11 | "Bill"  | "bill@main.com"        |
    ///     +----+---------+------------------------+

    pub fn print_table_data(&self) {

        let mut print_table = PrintTable::new();

        let column_names = self
            .columns
            .iter()
            .map(|col| col.column_name.to_string())
            .collect::<Vec<String>>();

        let header_row = PrintRow::new(
            column_names
                .iter()
                .map(|col| PrintCell::new(&col))
                .collect::<Vec<PrintCell>>(),
        );

        let rows_clone = Rc::clone(&self.rows);
        let row_data = rows_clone.as_ref().borrow();
        let first_col_data = row_data
            .get(&self.columns.first().unwrap().column_name)
            .unwrap();
        let num_rows = first_col_data.count();
        let mut print_table_rows: Vec<PrintRow> = vec![PrintRow::new(vec![]); num_rows];

        for col_name in &column_names {
            let col_val = row_data
                .get(col_name)
                .expect("Can't find any rows with the given column");
            let columns: Vec<String> = col_val.get_serialized_col_data();

            for i in 0..num_rows {
                if let Some(cell) = &columns.get(i) {
                    print_table_rows[i].add_cell(PrintCell::new(cell));
                } else {
                    print_table_rows[i].add_cell(PrintCell::new(""));
                }
            }
        }
        print_table.add_row(header_row);
        for row in print_table_rows {
            print_table.add_row(row);
        }
        print_table.printstd();
    }


    pub fn execute_select_query(&self,sq: &SelectQuery) {
            
        let mut print_table = PrintTable::new();

        let column_names = self
            .columns
            .iter()
            .map(|col| col.column_name.to_string())
            .collect::<Vec<String>>();

        let header_row = PrintRow::new(
            column_names
                .iter()
                .map(|col| PrintCell::new(&col))
                .collect::<Vec<PrintCell>>(),
        );

        let rows_clone = Rc::clone(&self.rows);
        let row_data = rows_clone.as_ref().borrow();
        let first_col_data = row_data
            .get(&self.columns.first().unwrap().column_name)
            .unwrap();
        let num_rows = first_col_data.count();
        let mut print_table_rows: Vec<PrintRow> = vec![PrintRow::new(vec![]); num_rows];

        for col_name in &column_names {
            let col_val = row_data
                .get(col_name)
                .expect("Can't find any rows with the given column");
            let columns: Vec<String> = col_val.get_serialized_col_data();

            for i in 0..num_rows {
                if let Some(cell) = &columns.get(i) {
                    print_table_rows[i].add_cell(PrintCell::new(cell));
                } else {
                    print_table_rows[i].add_cell(PrintCell::new(""));
                }
            }
        }
        print_table.add_row(header_row);
        for row in print_table_rows {
            print_table.add_row(row);
        }
        print_table.printstd();
    }

  
  
    //查询
    // pub fn execute_select_query(&self, sq: &SelectQuery) {
    //     let mut data: Vec<Vec<String>> = vec![];

    //     let expr = sq.where_expressions.first();
    //     match expr {
    //         Some(where_expr) => {
    //             let col = self.get_column(where_expr.left.to_string()).unwrap();

    //             if col.is_indexed {
    //                 println!("Executing select expression with index");
                
    //             } else {
    //                 println!("Executing select expression without index");
    //                 data = self.execute_select_query_without_index(sq);
    //             }
    //         }
    //         None => {
    //             println!("In none block");
    //             for col in &sq.projection {
    //                 let row = self.rows.get(col).unwrap();
    //                 let column = row.get_serialized_col_data();
    //                 data.push(column);
    //             }
    //         }
    //     }

    //     let rotated_data = Self::rotate_2d_vec(&data);
    //     Self::pretty_print(&rotated_data, &sq.projection);
    // }

   
    
 

}

//字段信息
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Column {
    /// 列的名称
    pub column_name: String,
    /// 列的类型
    pub datatype: DataType,
    /// 列是否是主键
    pub is_pk: bool,
    /// 能否为null
    pub not_null: bool,
    /// 值表示if列是用UNIQUE约束声明的
    pub is_unique: bool,
    ///值，表示列是否被索引
    pub is_indexed: bool,
    /// 将索引映射到相应行上的负载值的BtreeMap 使用ROWID映射
    pub index: ColumnIndex,
}

impl Column {
    pub fn new(
        name: String,
        datatype: String,
        is_pk: bool,
        not_null: bool,
        is_unique: bool,
    ) -> Self {
        let dt = DataType::new(datatype);
        let index = match dt {
            DataType::Integer => ColumnIndex::Integer(BTreeMap::new()),
            DataType::Bool => ColumnIndex::None,
            DataType::Text => ColumnIndex::Text(BTreeMap::new()),
            DataType::Real => ColumnIndex::None,
            DataType::Invalid => ColumnIndex::None,
            DataType::None => ColumnIndex::None,
        };

        Column {
            column_name: name,
            datatype: dt,
            is_pk,
            not_null,
            is_unique,
            is_indexed: if is_pk { true } else { false },
            index,
        }
    }

    pub fn get_mut_index(&mut self) -> &mut ColumnIndex {
        return &mut self.index;
    }
}

//每个表中每个SQL列索引的模式都在内存中表示
// 通过遵循结构
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ColumnIndex {
    // Integer(BTreeMap<i32, i64>),
    // Text(BTreeMap<String, i64>),
    // None,

    Integer(BTreeMap<i32, i64>),
    Text(BTreeMap<String, i64>),
    None,
}

impl ColumnIndex {
    // fn get_idx_data(&self, val: &String)  -> R<Option<usize>,String>{
    //     match self {
    //         ColumnIndex::Integer(index) => match val.parse::<i32>() {
    //             Ok(val) => Ok(index.get(&val)),
    //             Err(e) => Err(e.to_string()),
    //         },
          
    //         ColumnIndex::Text(index) => Ok(index.get(val)),
    //         ColumnIndex::None => Ok(None),
    //     };
    // }

    fn get_indexes_from_op<T: Clone>(val: T, op: Binary) -> (Bound<T>, Bound<T>) {
        match op {
            Binary::Eq => (Included(val.clone()), Included(val)),
            Binary::NotEq => (Excluded(val.clone()), Excluded(val)),
            Binary::Gt => (Excluded(val), Unbounded),
            Binary::GtEq => (Included(val), Unbounded),
            Binary::Lt => (Unbounded, Excluded(val)),
            Binary::LtEq => (Unbounded, Included(val)),
        }
    }

    fn get_idx_data_by_range(&self, val: &String, op: Binary) ->  R<Vec<usize>, String> {
        let mut indexes: Vec<usize> = vec![];
        match self {
            ColumnIndex::Integer(index) => match val.parse::<i32>() {
                Ok(val) => {
                    for (_, idx) in index.range(Self::get_indexes_from_op::<i32>(val, op)) {
                        indexes.push((*idx).try_into().unwrap());
                    }
                    Ok(indexes)
                }
                Err(e) => Err(e.to_string()),
            },

            // ColumnIndex::Bool(index) => match val.parse::<bool>() {
            //     Ok(val) => {
            //         for (_, idx) in index.range(Self::get_indexes_from_op::<bool>(val, op)) {
            //             indexes.push(*idx);
            //         }
            //         Ok(indexes)
            //     }
            //     Err(e) => Err(e.to_string()),
            // },
            ColumnIndex::Text(index) => {
                for (_, idx) in
                    index.range(Self::get_indexes_from_op::<String>(val.to_string(), op))
                {
                    indexes.push((*idx).try_into().unwrap());
                }
                Ok(indexes)
            }
            ColumnIndex::None => Ok(indexes),
        }
    }
}
//SQLRite数据类型
// 映射在SQLite数据类型存储类和SQLite亲和类型之后
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum DataType {
    Integer,
    Text,
    Real,
    Bool,
    None,
    Invalid,
}
//数据库类型设定
impl DataType {
    pub fn new(cmd: String) -> DataType {
        match cmd.to_lowercase().as_ref() {
            "integer" => DataType::Integer,
            "text" => DataType::Text,
            "real" => DataType::Real,
            "bool" => DataType::Bool,
            "none" => DataType::None,
            _ => {
                eprintln!("Invalid data type given {}", cmd);
                return DataType::Invalid;
            }
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            DataType::Integer => f.write_str("Integer"),
            DataType::Text => f.write_str("Text"),
            DataType::Real => f.write_str("Real"),
            DataType::Bool => f.write_str("Boolean"),
            DataType::None => f.write_str("None"),
            DataType::Invalid => f.write_str("Invalid"),
        }
    }
}
// 每个表中每个SQL行的模式都在内存中表示
// 通过遵循结构
// 这是一个枚举，表示BTreeMap中组织的每个可用类型
// 数据结构，使用ROWID和键以及每个对应的类型作为值
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Row {
    Integer(BTreeMap<i64, i32>),
    Text(BTreeMap<i64, String>),
    Real(BTreeMap<i64, f32>),
    Bool(BTreeMap<i64, bool>),
    None,
}

impl Row {
    //是row实现
    fn get_serialized_col_data(&self) -> Vec<String> {
        match self {
            Row::Integer(cd) => cd.iter().map(|(_i, v)| v.to_string()).collect(),
            Row::Real(cd) => cd.iter().map(|(_i, v)| v.to_string()).collect(),
            Row::Text(cd) => cd.iter().map(|(_i, v)| v.to_string()).collect(),
            Row::Bool(cd) => cd.iter().map(|(_i, v)| v.to_string()).collect(),
            Row::None => panic!("Found None in columns"),
        }
    }

    //count函数实现
    fn count(&self) -> usize {
        match self {
            Row::Integer(cd) => cd.len(),
            Row::Real(cd) => cd.len(),
            Row::Text(cd) => cd.len(),
            Row::Bool(cd) => cd.len(),
            Row::None => panic!("Found None in columns"),
        }
    }
}

    

    // fn get_serialized_col_data_by_index(&self, indices: &[usize]) -> Vec<String> {
    //     let mut selected_data = vec![];
    //     match self {
    //         Row::Integer(cd) => {
    //             indices
    //                 .iter()
    //                 .for_each(|i| selected_data.push((cd[*i]).to_string()));
    //         }
    //         Row::Real(cd) => {
    //             indices
    //                 .iter()
    //                 .for_each(|i| selected_data.push((cd[*i]).to_string()));
    //         }
    //         Row::Text(cd) => {
    //             indices
    //                 .iter()
    //                 .for_each(|i| selected_data.push((cd[*i]).to_string()));
    //         }
    //         Row::Bool(cd) => {
    //             indices
    //                 .iter()
    //                 .for_each(|i| selected_data.push((cd[*i]).to_string()));
    //         }
    //         Row::None => panic!("Found None in columns"),
    //     }
    //     selected_data
    // }

//     fn work<A, B, C, D>(
//         &self,
//         search_term: &str,
//         scanned_vals: &mut Vec<usize>,
//         func1: A,
//         func2: B,
//         func3: C,
//         func4: D,
//     ) -> Vec<usize>
//     where
//         A: Fn(i32, i32) -> bool,
//         B: Fn(f32, f32) -> bool,
//         C: Fn(&String, &String) -> bool,
//         D: Fn(bool, bool) -> bool,
//     {
//         match self {
//             Row::Integer(cd) => {
//                 let search_term = search_term.parse::<i32>().unwrap();

//                 for (idx, i) in cd.iter().enumerate() {
//                     if func1((*i).to, search_term) {
//                         scanned_vals.push(idx);
//                     }
//                 }
//                 scanned_vals.to_vec()
//             }
//             Row::Real(cd) => {
//                 let search_term = search_term.parse::<f32>().unwrap();

//                 for (idx, i) in cd.iter().enumerate() {
//                     if func2(*i, search_term) {
//                         scanned_vals.push(idx);
//                     }
//                 }
//                 scanned_vals.to_vec()
//             }
//             Row::Text(cd) => {
//                 let search_term = search_term.parse::<String>().unwrap();

//                 for (idx, i) in cd.iter().enumerate() {
//                     if func3(i, &search_term) {
//                         scanned_vals.push(idx);
//                     }
//                 }
//                 scanned_vals.to_vec()
//             }
//             Row::Bool(cd) => {
//                 let search_term = search_term.parse::<bool>().unwrap();
//                 for (idx, i) in cd.iter().enumerate() {
//                     if func4(*i, search_term) {
//                         scanned_vals.push(idx);
//                     }
//                 }
//                 scanned_vals.to_vec()
//             }
//             Row::None => panic!("Found None in columns"),
//         }
//     }

//     fn get_serialized_col_data_by_scanning(&self, expr: &Expression) -> Vec<usize> {
//         let search_term = (expr.right).to_string();
//         let mut scanned_vals = vec![];
//         match &expr.op {
//             Operator::Binary(binary_op) => match binary_op {
//                 Binary::NotEq => Self::work(
//                     self,
//                     &search_term,
//                     &mut scanned_vals,
//                     |a, b| a != b,
//                     |a, b| a != b,
//                     |a, b| a != b,
//                     |a, b| a != b,
//                 ),
//                 Binary::Eq => self.work(
//                     &search_term,
//                     &mut scanned_vals,
//                     |a, b| a == b,
//                     |a, b| a == b,
//                     |a, b| a == b,
//                     |a, b| a == b,
//                 ),
//                 Binary::Gt => self.work(
//                     &search_term,
//                     &mut scanned_vals,
//                     |a, b| a > b,
//                     |a, b| a > b,
//                     |a, b| a > b,
//                     |a, b| a & !b,
//                 ),
//                 Binary::Lt => self.work(
//                     &search_term,
//                     &mut scanned_vals,
//                     |a, b| a < b,
//                     |a, b| a < b,
//                     |a, b| a < b,
//                     |a, b| !a & b,
//                 ),
//                 Binary::LtEq => self.work(
//                     &search_term,
//                     &mut scanned_vals,
//                     |a, b| a <= b,
//                     |a, b| a <= b,
//                     |a, b| a <= b,
//                     |a, b| a <= b,
//                 ),
//                 Binary::GtEq => self.work(
//                     &search_term,
//                     &mut scanned_vals,
//                     |a, b| a >= b,
//                     |a, b| a >= b,
//                     |a, b| a >= b,
//                     |a, b| a >= b,
//                 ),
//             },
//         }
//     }
// }
