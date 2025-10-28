// Planner module - converts AST into execution plans

use crate::parser::Statement;

/// Query execution plan
#[derive(Debug)]
pub enum Plan {
    CreateTable {
        table_name: String,
        columns: Vec<crate::parser::Column>,
    },
    CreateIndex {
        table_name: String,
        column_name: String,
    },
    Insert {
        table_name: String,
        values: Vec<crate::parser::Value>,
    },
    Scan {
        table_name: String,
        columns: Vec<String>,
        filter: Option<crate::parser::WhereClause>,
    },
    Delete {
        table_name: String,
        filter: Option<crate::parser::WhereClause>,
    },
    Update {
        table_name: String,
        column: String,
        value: crate::parser::Value,
        filter: Option<crate::parser::WhereClause>,
    },
}

/// Convert Statement to Plan
pub fn plan(statement: Statement) -> Result<Plan, String> {
    match statement {
        Statement::CreateTable { table_name, columns } => {
            Ok(Plan::CreateTable { table_name, columns })
        }
        Statement::CreateIndex { table_name, column_name } => {
            Ok(Plan::CreateIndex { table_name, column_name })
        }
        Statement::Insert { table_name, values } => {
            Ok(Plan::Insert { table_name, values })
        }
        Statement::Select { table_name, columns, where_clause } => {
            Ok(Plan::Scan {
                table_name,
                columns,
                filter: where_clause,
            })
        }
        Statement::Delete { table_name, where_clause } => {
            Ok(Plan::Delete {
                table_name,
                filter: where_clause,
            })
        }
        Statement::Update { table_name, column, value, where_clause } => {
            Ok(Plan::Update {
                table_name,
                column,
                value,
                filter: where_clause,
            })
        }
    }
}