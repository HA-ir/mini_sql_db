// Storage module - manages tables and data

use crate::parser::{Column, Value, WhereClause, Operator};
use std::collections::HashMap;

pub mod btree;
pub mod disk;

use btree::Index;

/// Represents a table in the database
#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Value>>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            rows: Vec::new(),
        }
    }

    /// Get column index by name
    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == column_name)
    }
}

/// In-memory database
pub struct Database {
    tables: HashMap<String, Table>,
    indexes: HashMap<String, HashMap<String, Index>>, // table_name -> column_name -> Index
}

impl Database {
    /// Create a new empty database
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
        }
    }

    /// Load database from disk
    pub fn load_from_disk() -> Result<Self, String> {
        let tables_vec = disk::load_all_tables()
            .map_err(|e| format!("Failed to load tables: {}", e))?;

        let mut tables = HashMap::new();
        for table in tables_vec {
            tables.insert(table.name.clone(), table);
        }

        Ok(Self {
            tables,
            indexes: HashMap::new(),
        })
    }

    /// Save database to disk
    pub fn save_to_disk(&self) -> Result<(), String> {
        for table in self.tables.values() {
            disk::save_table(table)
                .map_err(|e| format!("Failed to save table '{}': {}", table.name, e))?;
        }
        Ok(())
    }

    /// Create a new table
    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }

        let table = Table::new(name.clone(), columns);
        
        // Save to disk
        disk::save_table(&table)
            .map_err(|e| format!("Failed to save table: {}", e))?;

        self.tables.insert(name, table);
        Ok(())
    }

    /// Create an index on a column
    pub fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<(), String> {
        let table = self.tables.get(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let column_index = table.get_column_index(column_name)
            .ok_or_else(|| format!("Column '{}' does not exist", column_name))?;

        // Create index
        let mut index = Index::new(column_name.to_string(), column_index);
        index.build(&table.rows);

        // Store index
        self.indexes
            .entry(table_name.to_string())
            .or_insert_with(HashMap::new)
            .insert(column_name.to_string(), index);

        Ok(())
    }

    /// Insert a row into a table
    pub fn insert_row(&mut self, table_name: &str, values: Vec<Value>) -> Result<(), String> {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        if values.len() != table.columns.len() {
            return Err(format!(
                "Expected {} values, got {}",
                table.columns.len(),
                values.len()
            ));
        }

        // Validate types
        for (value, column) in values.iter().zip(table.columns.iter()) {
            match (value, &column.data_type) {
                (Value::Int(_), crate::parser::DataType::Int) => {}
                (Value::Text(_), crate::parser::DataType::Text) => {}
                (Value::Float(_), crate::parser::DataType::Float) => {}
                (Value::Null, _) => {}
                _ => {
                    return Err(format!(
                        "Type mismatch for column '{}': expected {:?}, got {:?}",
                        column.name, column.data_type, value
                    ));
                }
            }
        }

        let row_idx = table.rows.len();
        table.rows.push(values.clone());

        // Update indexes
        if let Some(table_indexes) = self.indexes.get_mut(table_name) {
            for index in table_indexes.values_mut() {
                if let Some(value) = values.get(index.column_index) {
                    index.insert(row_idx, value);
                }
            }
        }

        // Save to disk
        disk::save_table(table)
            .map_err(|e| format!("Failed to save table: {}", e))?;

        Ok(())
    }

    /// Delete rows from a table based on filter
    pub fn delete_rows(&mut self, table_name: &str, filter: Option<&WhereClause>) -> Result<usize, String> {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let indices_to_delete = if let Some(where_clause) = filter {
            // Get column index
            let col_idx = table.get_column_index(&where_clause.column)
                .ok_or_else(|| format!("Column '{}' does not exist", where_clause.column))?;

            // Find matching rows
            table.rows.iter()
                .enumerate()
                .filter(|(_, row)| {
                    if let Some(value) = row.get(col_idx) {
                        compare_values(value, &where_clause.operator, &where_clause.value)
                    } else {
                        false
                    }
                })
                .map(|(idx, _)| idx)
                .collect::<Vec<_>>()
        } else {
            // Delete all rows
            (0..table.rows.len()).collect()
        };

        let count = indices_to_delete.len();

        // Remove rows in reverse order to maintain indices
        for &idx in indices_to_delete.iter().rev() {
            table.rows.remove(idx);
        }

        // Rebuild all indexes for this table
        if let Some(table_indexes) = self.indexes.get_mut(table_name) {
            for index in table_indexes.values_mut() {
                index.build(&table.rows);
            }
        }

        // Save to disk
        disk::save_table(table)
            .map_err(|e| format!("Failed to save table: {}", e))?;

        Ok(count)
    }

    /// Update rows in a table
    pub fn update_rows(
        &mut self,
        table_name: &str,
        column_name: &str,
        new_value: Value,
        filter: Option<&WhereClause>
    ) -> Result<usize, String> {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        // Get the column index to update
        let update_col_idx = table.get_column_index(column_name)
            .ok_or_else(|| format!("Column '{}' does not exist", column_name))?;

        // Validate the new value type
        let expected_type = &table.columns[update_col_idx].data_type;
        match (&new_value, expected_type) {
            (Value::Int(_), crate::parser::DataType::Int) => {}
            (Value::Text(_), crate::parser::DataType::Text) => {}
            (Value::Float(_), crate::parser::DataType::Float) => {}
            (Value::Null, _) => {}
            _ => {
                return Err(format!(
                    "Type mismatch for column '{}': expected {:?}, got {:?}",
                    column_name, expected_type, new_value
                ));
            }
        }

        let mut count = 0;

        if let Some(where_clause) = filter {
            // Get column index for filter
            let filter_col_idx = table.get_column_index(&where_clause.column)
                .ok_or_else(|| format!("Column '{}' does not exist", where_clause.column))?;

            // Update matching rows
            for row in &mut table.rows {
                if let Some(value) = row.get(filter_col_idx) {
                    if compare_values(value, &where_clause.operator, &where_clause.value) {
                        row[update_col_idx] = new_value.clone();
                        count += 1;
                    }
                }
            }
        } else {
            // Update all rows
            for row in &mut table.rows {
                row[update_col_idx] = new_value.clone();
                count += 1;
            }
        }

        // Rebuild indexes if the updated column is indexed
        if let Some(table_indexes) = self.indexes.get_mut(table_name) {
            if table_indexes.contains_key(column_name) {
                // Rebuild all indexes to be safe
                for index in table_indexes.values_mut() {
                    index.build(&table.rows);
                }
            }
        }

        // Save to disk
        disk::save_table(table)
            .map_err(|e| format!("Failed to save table: {}", e))?;

        Ok(count)
    }

    /// Select all columns from a table
    pub fn select_all(&self, table_name: &str) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
        let table = self.tables.get(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let column_names: Vec<String> = table.columns.iter()
            .map(|c| c.name.clone())
            .collect();

        Ok((column_names, table.rows.clone()))
    }

    /// Select with specific columns and optional filter
    pub fn select_with_filter(
        &self,
        table_name: &str,
        columns: Vec<String>,
        filter: Option<&WhereClause>,
    ) -> Result<(Vec<String>, Vec<Vec<Value>>), String> {
        let table = self.tables.get(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        // Validate and get column indices
        let col_indices: Result<Vec<usize>, String> = if columns.is_empty() {
            Ok((0..table.columns.len()).collect())
        } else {
            columns.iter()
                .map(|name| {
                    table.get_column_index(name)
                        .ok_or_else(|| format!("Column '{}' does not exist", name))
                })
                .collect()
        };
        let col_indices = col_indices?;

        let column_names = if columns.is_empty() {
            table.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            columns
        };

        // Apply filter
        let filtered_rows = if let Some(where_clause) = filter {
            self.filter_rows(table, where_clause)?
        } else {
            table.rows.clone()
        };

        // Project columns
        let result_rows: Vec<Vec<Value>> = filtered_rows.iter()
            .map(|row| {
                col_indices.iter()
                    .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                    .collect()
            })
            .collect();

        Ok((column_names, result_rows))
    }

    /// Filter rows based on WHERE clause
    fn filter_rows(&self, table: &Table, where_clause: &WhereClause) -> Result<Vec<Vec<Value>>, String> {
        let col_idx = table.get_column_index(&where_clause.column)
            .ok_or_else(|| format!("Column '{}' does not exist", where_clause.column))?;

        // Try to use index if available
        if let Some(table_indexes) = self.indexes.get(&table.name) {
            if let Some(index) = table_indexes.get(&where_clause.column) {
                return self.filter_with_index(table, index, where_clause);
            }
        }

        // Fallback to table scan
        Ok(table.rows.iter()
            .filter(|row| {
                if let Some(value) = row.get(col_idx) {
                    compare_values(value, &where_clause.operator, &where_clause.value)
                } else {
                    false
                }
            })
            .cloned()
            .collect())
    }

    /// Filter using an index
    fn filter_with_index(
        &self,
        table: &Table,
        index: &Index,
        where_clause: &WhereClause,
    ) -> Result<Vec<Vec<Value>>, String> {
        let row_indices = match &where_clause.operator {
            Operator::Equals => {
                index.lookup(&where_clause.value)
                    .map(|v| v.clone())
                    .unwrap_or_default()
            }
            Operator::GreaterThan => index.greater_than(&where_clause.value),
            Operator::LessThan => index.less_than(&where_clause.value),
            _ => {
                // For other operators, fall back to table scan
                return self.filter_rows(table, where_clause);
            }
        };

        Ok(row_indices.iter()
            .filter_map(|&idx| table.rows.get(idx).cloned())
            .collect())
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
}

/// Compare two values using an operator
fn compare_values(left: &Value, operator: &Operator, right: &Value) -> bool {
    match operator {
        Operator::Equals => left == right,
        Operator::NotEquals => left != right,
        Operator::GreaterThan => match (left, right) {
            (Value::Int(a), Value::Int(b)) => a > b,
            (Value::Float(a), Value::Float(b)) => a > b,
            (Value::Text(a), Value::Text(b)) => a > b,
            _ => false,
        },
        Operator::LessThan => match (left, right) {
            (Value::Int(a), Value::Int(b)) => a < b,
            (Value::Float(a), Value::Float(b)) => a < b,
            (Value::Text(a), Value::Text(b)) => a < b,
            _ => false,
        },
        Operator::GreaterOrEqual => match (left, right) {
            (Value::Int(a), Value::Int(b)) => a >= b,
            (Value::Float(a), Value::Float(b)) => a >= b,
            (Value::Text(a), Value::Text(b)) => a >= b,
            _ => false,
        },
        Operator::LessOrEqual => match (left, right) {
            (Value::Int(a), Value::Int(b)) => a <= b,
            (Value::Float(a), Value::Float(b)) => a <= b,
            (Value::Text(a), Value::Text(b)) => a <= b,
            _ => false,
        },
    }
}