use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, RwLock},
};

use crate::{
    id::{Indexed, RowId},
    index::{Index, IndexRead, Indexable},
};

pub struct HashSync<'a, RowT> {
    rows: Arc<RwLock<HashMap<RowId, RowT>>>,
    next_id: RowId,
    indexes: Vec<Box<dyn Indexable<RowT> + 'a>>,
}

impl<'a, RowT: Clone + 'a> HashSync<'a, RowT> {
    pub fn new() -> Self {
        HashSync {
            rows: Arc::new(RwLock::new(HashMap::new())),
            next_id: RowId::new(0),
            indexes: Vec::new(),
        }
    }

    pub fn insert(&mut self, row: RowT) -> RowId {
        let id = self.next_id;
        let mut rows_guard = self.rows.write().unwrap();
        for index in self.indexes.iter_mut() {
            index.insert(Indexed::new(id, row.clone()));
        }
        rows_guard.insert(id, row);
        self.next_id = self.next_id.next();
        id
    }

    pub fn delete(&mut self, id: RowId) {
        let mut rows_guard = self.rows.write().unwrap();
        let row = rows_guard.remove(&id);
        if let Some(row) = row {
            for index in self.indexes.iter_mut() {
                index.delete(Indexed::new(id, row.clone()));
            }
        }
        rows_guard.remove(&id);
    }

    pub fn index<IndexKeyT, IndexFn>(&mut self, index_fn: IndexFn) -> IndexRead<IndexKeyT, RowT>
    where
        IndexFn: Fn(&RowT) -> IndexKeyT + 'static,
        IndexKeyT: PartialEq + Eq + Hash + 'a,
    {
        let mut index = Index::new(Box::new(index_fn));
        let rows_guard = self.rows.read().unwrap();
        for row in rows_guard.iter() {
            index.insert(Indexed::new(*row.0, row.1.clone()));
        }
        let (index_read, index_write) = index.into_read_write(self.rows.clone());
        self.indexes.push(Box::new(index_write));
        index_read
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_once() {
        let mut hs = HashSync::new();
        hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 4));
        let index = hs.index(|&(a, _b)| a);

        let rows = index.get_values(&1);
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&(1, 2)));
        assert!(rows.contains(&(1, 3)));
    }

    #[test]
    fn insert_twice() {
        let mut hs = HashSync::new();
        hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 4));
        let index = hs.index(|&(a, _b)| a);

        hs.insert((1, 4));
        let rows = index.get_values(&1);
        assert_eq!(rows.len(), 3);
        assert!(rows.contains(&(1, 2)));
        assert!(rows.contains(&(1, 3)));
        assert!(rows.contains(&(1, 4)));
    }

    #[test]
    fn delete() {
        let mut hs = HashSync::new();
        let row_to_delete = hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 4));
        let index = hs.index(|&(a, _b)| a);

        hs.delete(row_to_delete);
        let rows = index.get_values(&1);
        assert_eq!(rows.len(), 1);
        assert!(rows.contains(&(1, 3)));
    }

    #[test]
    fn two_indexes() {
        let mut hs = HashSync::new();
        hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 2));
        let index1 = hs.index(|&(a, _b)| a);
        let index2 = hs.index(|&(_a, b)| b);

        let rows1 = index1.get_values(&1);
        assert_eq!(rows1.len(), 2);
        assert!(rows1.contains(&(1, 2)));
        assert!(rows1.contains(&(1, 3)));

        let rows2 = index2.get_values(&2);
        assert_eq!(rows2.len(), 2);
        assert!(rows2.contains(&(1, 2)));
        assert!(rows2.contains(&(3, 2)));
    }

    #[test]
    fn two_indexes_with_delete() {
        let mut hs = HashSync::new();
        hs.insert((1, 2));
        let row_to_delete = hs.insert((1, 3));
        hs.insert((3, 2));
        let index1 = hs.index(|&(a, _b)| a);
        let index2 = hs.index(|&(_a, b)| b);

        let rows1 = index1.get_values(&1);
        assert_eq!(rows1.len(), 2);
        assert!(rows1.contains(&(1, 2)));
        assert!(rows1.contains(&(1, 3)));

        let rows2 = index2.get_values(&2);
        assert_eq!(rows2.len(), 2);
        assert!(rows2.contains(&(1, 2)));
        assert!(rows2.contains(&(3, 2)));

        hs.delete(row_to_delete);

        let rows1 = index1.get_values(&1);
        assert_eq!(rows1.len(), 1);
        assert!(rows1.contains(&(1, 2)));

        let rows2 = index2.get_values(&2);
        assert_eq!(rows2.len(), 2);
        assert!(rows2.contains(&(1, 2)));
        assert!(rows2.contains(&(3, 2)));
    }
}
