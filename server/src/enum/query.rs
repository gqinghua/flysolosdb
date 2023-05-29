pub type ColName = String;

#[derive(Debug, PartialEq, Eq)]
pub enum DatabaseAction {
    Create,
    Drop,
    Use,
}

#[derive(Debug, PartialEq, Eq)]
pub enum TableQuery {
    Create {
        cols: Vec<String>,
        types: Vec<DataType>,
    },
    DropTable,
    Truncate,
    AddCol {
        col_name: String,
        datatype: DataType,
    },
    AlterCol {
        col_name: String,
        datatype: DataType,
    },
    DropCol(ColName),
    Select {
        cols: SelectCols,
        condition: Option<Condition>,
    },
    Insert {
        cols: SelectCols,
        values: Vec<Vec<String>>,
    },
    Delete {
        condition: Condition,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum SelectCols {
    All,
    Cols(Vec<String>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Query {
    ShowAllDBs,
    ShowCurrDB,
    ShowTables,
    Database {
        name: String,
        action: DatabaseAction,
    },
    Table {
        name: String,
        query: TableQuery,
    },
}
