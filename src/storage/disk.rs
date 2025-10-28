// Disk persistence module

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use crate::parser::{Column, DataType, Value};
use super::Table;

const DATA_DIR: &str = "data";
const TABLE_EXTENSION: &str = ".tbl";

/// Initialize data directory
pub fn init_data_dir() -> io::Result<()> {
    fs::create_dir_all(DATA_DIR)?;
    Ok(())
}

/// Save a table to disk
pub fn save_table(table: &Table) -> io::Result<()> {
    init_data_dir()?;
    
    let path = get_table_path(&table.name);
    let mut file = File::create(path)?;

    // Write schema: column_name:type,column_name:type,...
    let schema: Vec<String> = table.columns.iter()
        .map(|col| format!("{}:{}", col.name, datatype_to_string(&col.data_type)))
        .collect();
    writeln!(file, "{}", schema.join(","))?;

    // Write rows: value|value|value
    for row in &table.rows {
        let row_str: Vec<String> = row.iter()
            .map(value_to_string)
            .collect();
        writeln!(file, "{}", row_str.join("|"))?;
    }

    Ok(())
}

/// Load a table from disk
pub fn load_table(table_name: &str) -> io::Result<Table> {
    let path = get_table_path(table_name);
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Read schema line
    let mut schema_line = String::new();
    reader.read_line(&mut schema_line)?;
    let schema_line = schema_line.trim();

    let columns = parse_schema(schema_line)?;

    // Read data lines
    let mut rows = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let row = parse_row(&line, &columns)?;
        rows.push(row);
    }

    Ok(Table {
        name: table_name.to_string(),
        columns,
        rows,
    })
}

/// Load all tables from disk
pub fn load_all_tables() -> io::Result<Vec<Table>> {
    init_data_dir()?;
    
    let mut tables = Vec::new();
    
    for entry in fs::read_dir(DATA_DIR)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("tbl") {
            if let Some(table_name) = path.file_stem().and_then(|s| s.to_str()) {
                match load_table(table_name) {
                    Ok(table) => tables.push(table),
                    Err(e) => eprintln!("Failed to load table '{}': {}", table_name, e),
                }
            }
        }
    }
    
    Ok(tables)
}

/// Delete a table file from disk
pub fn delete_table(table_name: &str) -> io::Result<()> {
    let path = get_table_path(table_name);
    fs::remove_file(path)
}

/// Get the file path for a table
fn get_table_path(table_name: &str) -> PathBuf {
    Path::new(DATA_DIR).join(format!("{}{}", table_name, TABLE_EXTENSION))
}

/// Parse schema line into columns
fn parse_schema(schema_line: &str) -> io::Result<Vec<Column>> {
    let mut columns = Vec::new();
    
    for col_def in schema_line.split(',') {
        let parts: Vec<&str> = col_def.split(':').collect();
        if parts.len() != 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid column definition: {}", col_def),
            ));
        }
        
        let name = parts[0].to_string();
        let data_type = string_to_datatype(parts[1])?;
        
        columns.push(Column { name, data_type });
    }
    
    Ok(columns)
}

/// Parse a data row
fn parse_row(line: &str, columns: &[Column]) -> io::Result<Vec<Value>> {
    let parts: Vec<&str> = line.split('|').collect();
    
    if parts.len() != columns.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Expected {} values, got {}", columns.len(), parts.len()),
        ));
    }
    
    let mut row = Vec::new();
    for (val_str, col) in parts.iter().zip(columns.iter()) {
        let value = string_to_value(val_str, &col.data_type)?;
        row.push(value);
    }
    
    Ok(row)
}

/// Convert DataType to string
fn datatype_to_string(dt: &DataType) -> &str {
    match dt {
        DataType::Int => "INT",
        DataType::Text => "TEXT",
        DataType::Float => "FLOAT",
    }
}

/// Convert string to DataType
fn string_to_datatype(s: &str) -> io::Result<DataType> {
    match s {
        "INT" => Ok(DataType::Int),
        "TEXT" => Ok(DataType::Text),
        "FLOAT" => Ok(DataType::Float),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unknown data type: {}", s),
        )),
    }
}

/// Convert Value to string for storage
fn value_to_string(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Text(s) => escape_string(s),
        Value::Float(f) => f.to_string(),
        Value::Null => "NULL".to_string(),
    }
}

/// Convert string to Value based on data type
fn string_to_value(s: &str, data_type: &DataType) -> io::Result<Value> {
    if s == "NULL" {
        return Ok(Value::Null);
    }
    
    match data_type {
        DataType::Int => {
            s.parse::<i64>()
                .map(Value::Int)
                .map_err(|_| io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid integer: {}", s),
                ))
        }
        DataType::Text => Ok(Value::Text(unescape_string(s))),
        DataType::Float => {
            s.parse::<f64>()
                .map(Value::Float)
                .map_err(|_| io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Invalid float: {}", s),
                ))
        }
    }
}

/// Escape special characters in strings
fn escape_string(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Unescape special characters in strings
fn unescape_string(s: &str) -> String {
    let mut result = String::new();
    let mut chars = s.chars();
    
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            if let Some(next) = chars.next() {
                match next {
                    '\\' => result.push('\\'),
                    '|' => result.push('|'),
                    'n' => result.push('\n'),
                    'r' => result.push('\r'),
                    _ => {
                        result.push('\\');
                        result.push(next);
                    }
                }
            } else {
                result.push('\\');
            }
        } else {
            result.push(ch);
        }
    }
    
    result
}