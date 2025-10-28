// B-tree index implementation for fast lookups

use std::collections::BTreeMap;
use crate::parser::Value;

/// Index on a specific column
pub struct Index {
    pub column_name: String,
    pub column_index: usize,
    // Maps value to row indices
    pub tree: BTreeMap<IndexKey, Vec<usize>>,
}

/// Wrapper for Value that implements Ord for use in BTreeMap
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IndexKey {
    Int(i64),
    Text(String),
    Float(OrderedFloat),
    Null,
}

/// Wrapper for f64 to make it Ord (treats NaN as less than everything)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedFloat(f64);

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Less)
    }
}

impl From<&Value> for IndexKey {
    fn from(value: &Value) -> Self {
        match value {
            Value::Int(n) => IndexKey::Int(*n),
            Value::Text(s) => IndexKey::Text(s.clone()),
            Value::Float(f) => IndexKey::Float(OrderedFloat(*f)),
            Value::Null => IndexKey::Null,
        }
    }
}

impl Index {
    /// Create a new index on a column
    pub fn new(column_name: String, column_index: usize) -> Self {
        Self {
            column_name,
            column_index,
            tree: BTreeMap::new(),
        }
    }

    /// Build index from existing rows
    pub fn build(&mut self, rows: &[Vec<Value>]) {
        self.tree.clear();
        
        for (row_idx, row) in rows.iter().enumerate() {
            if let Some(value) = row.get(self.column_index) {
                let key = IndexKey::from(value);
                self.tree.entry(key)
                    .or_insert_with(Vec::new)
                    .push(row_idx);
            }
        }
    }

    /// Insert a new row into the index
    pub fn insert(&mut self, row_idx: usize, value: &Value) {
        let key = IndexKey::from(value);
        self.tree.entry(key)
            .or_insert_with(Vec::new)
            .push(row_idx);
    }

    /// Lookup rows by exact value
    pub fn lookup(&self, value: &Value) -> Option<&Vec<usize>> {
        let key = IndexKey::from(value);
        self.tree.get(&key)
    }

    /// Range query: find all rows with values in [start, end]
    pub fn range_lookup(&self, start: &Value, end: &Value) -> Vec<usize> {
        let start_key = IndexKey::from(start);
        let end_key = IndexKey::from(end);
        
        let mut result = Vec::new();
        
        for (_, row_indices) in self.tree.range(start_key..=end_key) {
            result.extend_from_slice(row_indices);
        }
        
        result
    }

    /// Get all row indices greater than a value
    pub fn greater_than(&self, value: &Value) -> Vec<usize> {
        let key = IndexKey::from(value);
        
        let mut result = Vec::new();
        
        for (_, row_indices) in self.tree.range((std::ops::Bound::Excluded(key), std::ops::Bound::Unbounded)) {
            result.extend_from_slice(row_indices);
        }
        
        result
    }

    /// Get all row indices less than a value
    pub fn less_than(&self, value: &Value) -> Vec<usize> {
        let key = IndexKey::from(value);
        
        let mut result = Vec::new();
        
        for (_, row_indices) in self.tree.range(..key) {
            result.extend_from_slice(row_indices);
        }
        
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_basic() {
        let mut index = Index::new("id".to_string(), 0);
        
        let rows = vec![
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            vec![Value::Int(2), Value::Text("Bob".to_string())],
            vec![Value::Int(3), Value::Text("Charlie".to_string())],
        ];
        
        index.build(&rows);
        
        assert_eq!(index.lookup(&Value::Int(2)), Some(&vec![1]));
        assert_eq!(index.lookup(&Value::Int(99)), None);
    }
    
    #[test]
    fn test_index_range() {
        let mut index = Index::new("id".to_string(), 0);
        
        let rows = vec![
            vec![Value::Int(1)],
            vec![Value::Int(5)],
            vec![Value::Int(10)],
            vec![Value::Int(15)],
        ];
        
        index.build(&rows);
        
        let result = index.range_lookup(&Value::Int(5), &Value::Int(10));
        assert_eq!(result, vec![1, 2]);
    }
}