use std::fs;
use std::path::Path;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

pub const DB_DIR: &str = "./data";
pub const CURR_DB: &str = "curr_db";
fn main() {
    let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

    let sql2 = "select * from users;";
    let sql3 = "use users";
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast = Parser::parse_sql(&dialect, sql3).unwrap();
    let enums = ast.get(0);
    for i in &ast {
        println!("{i}");
    }
    println!("enums: {:?}", enums);
    println!("AST: {:?}", ast);
    println!("Hello, world!");
    let name = "user";

    let base_dir = Path::new(DB_DIR);
    let db_dir = base_dir.join(name);
    fs::create_dir_all(db_dir).unwrap();
    
}
