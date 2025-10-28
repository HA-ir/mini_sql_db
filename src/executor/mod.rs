use crate::planner::Plan;
use crate::storage::Database;
use crate::parser::Value;

/// Result of a query execution
#[derive(Debug)]
pub enum ExecutionResult {
    Success(String),
    Rows { columns: Vec<String>, rows: Vec<Vec<Value>> },
}

/// Execute a query plan
pub fn execute(plan: Plan, db: &mut Database) -> Result<ExecutionResult, String> {
    match plan {
        Plan::CreateTable { table_name, columns } => {
            db.create_table(table_name.clone(), columns)?;
            Ok(ExecutionResult::Success(format!(
                "Table '{}' created successfully",
                table_name
            )))
        }
        Plan::CreateIndex { table_name, column_name } => {
            db.create_index(&table_name, &column_name)?;
            Ok(ExecutionResult::Success(format!(
                "Index created on column '{}' of table '{}'",
                column_name, table_name
            )))
        }
        Plan::Insert { table_name, values } => {
            db.insert_row(&table_name, values)?;
            Ok(ExecutionResult::Success("1 row inserted".to_string()))
        }
        Plan::Scan { table_name, columns, filter } => {
            let (col_names, rows) = if columns.is_empty() {
                db.select_all(&table_name)?
            } else {
                db.select_with_filter(&table_name, columns, filter.as_ref())?
            };

            Ok(ExecutionResult::Rows {
                columns: col_names,
                rows,
            })
        }
        Plan::Delete { table_name, filter } => {
            let count = db.delete_rows(&table_name, filter.as_ref())?;
            Ok(ExecutionResult::Success(format!("{} row(s) deleted", count)))
        }
        Plan::Update { table_name, column, value, filter } => {
            let count = db.update_rows(&table_name, &column, value, filter.as_ref())?;
            Ok(ExecutionResult::Success(format!("{} row(s) updated", count)))
        }
    }
}

/// Format execution results
pub fn format_results(result: ExecutionResult) -> String {
    match result {
        ExecutionResult::Success(msg) => msg,
        ExecutionResult::Rows { columns, rows } => {
            if rows.is_empty() {
                return "No rows returned".to_string();
            }
            format_table(&columns, &rows)
        }
    }
}

/// Format rows as ASCII table
fn format_table(columns: &[String], rows: &[Vec<Value>]) -> String {
    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    
    for row in rows {
        for (i, val) in row.iter().enumerate() {
            if i < widths.len() {
                let val_str = value_to_string(val);
                if val_str.len() > widths[i] {
                    widths[i] = val_str.len();
                }
            }
        }
    }

    // Build table
    let mut output = String::new();
    
    // Top border
    output.push('+');
    for width in &widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Header
    output.push('|');
    for (i, col) in columns.iter().enumerate() {
        output.push_str(&format!(" {:width$} ", col, width = widths[i]));
        output.push('|');
    }
    output.push('\n');

    // Middle border
    output.push('+');
    for width in &widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Rows
    for row in rows {
        output.push('|');
        for (i, val) in row.iter().enumerate() {
            let val_str = value_to_string(val);
            output.push_str(&format!(" {:width$} ", val_str, width = widths[i]));
            output.push('|');
        }
        output.push('\n');
    }

    // Bottom border
    output.push('+');
    for width in &widths {
        output.push_str(&"-".repeat(width + 2));
        output.push('+');
    }
    output.push('\n');

    // Add row count
    output.push_str(&format!("{} row(s) returned\n", rows.len()));

    output
}

/// Convert Value to display string
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Text(s) => s.clone(),
        Value::Float(f) => format!("{:.2}", f),
        Value::Null => "NULL".to_string(),
    }
}