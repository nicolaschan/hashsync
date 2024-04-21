use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{Arc, RwLock},
};

use crate::id::{Indexed, RowId};

pub struct IndexId(usize);

impl IndexId {
    pub fn new(id: usize) -> Self {
        IndexId(id)
    }

    pub fn next(&self) -> Self {
        IndexId(self.0 + 1)
    }
}

pub trait Indexable<ValueT> {
    fn insert(&mut self, row: Indexed<ValueT>) -> IndexId;
    fn delete(&mut self, row: Indexed<ValueT>);
}

pub struct Index<KeyT, ValueT> {
    index_function: Box<dyn Fn(&ValueT) -> Vec<KeyT>>,
    index: HashMap<KeyT, HashSet<RowId>>,
}

impl<KeyT: PartialEq + Eq + Hash, ValueT: Clone> Index<KeyT, ValueT> {
    pub fn new(index_function: Box<dyn Fn(&ValueT) -> Vec<KeyT>>) -> Self {
        Index {
            index_function,
            index: HashMap::new(),
        }
    }

    pub fn get(&self, key: &KeyT) -> HashSet<RowId> {
        self.index.get(key).cloned().unwrap_or_default()
    }

    pub fn into_read_write(
        self,
        rows: Arc<RwLock<HashMap<RowId, ValueT>>>,
    ) -> (IndexRead<KeyT, ValueT>, IndexWrite<KeyT, ValueT>) {
        let index = Arc::new(RwLock::new(self));
        (IndexRead::new(rows, index.clone()), IndexWrite::new(index))
    }
}

impl<KeyT: PartialEq + Eq + Hash, ValueT> Indexable<ValueT> for Index<KeyT, ValueT> {
    fn insert(&mut self, row: Indexed<ValueT>) -> IndexId {
        let keys = (self.index_function)(&row.value());
        for key in keys {
            self.index
                .entry(key)
                .or_insert(HashSet::new())
                .insert(row.id());
        }
        IndexId::new(0)
    }

    fn delete(&mut self, row: Indexed<ValueT>) {
        let keys = (self.index_function)(&row.value());
        for key in keys {
            if let Some(set) = self.index.get_mut(&key) {
                set.remove(&row.id());
            }
        }
    }
}

pub struct IndexRead<KeyT, ValueT> {
    rows: Arc<RwLock<HashMap<RowId, ValueT>>>,
    index: Arc<RwLock<Index<KeyT, ValueT>>>,
}

impl<KeyT: PartialEq + Eq + Hash, ValueT: Clone> IndexRead<KeyT, ValueT> {
    pub fn new(
        rows: Arc<RwLock<HashMap<RowId, ValueT>>>,
        index: Arc<RwLock<Index<KeyT, ValueT>>>,
    ) -> Self {
        IndexRead { rows, index }
    }

    pub fn get(&self, key: &KeyT) -> Vec<Indexed<ValueT>> {
        let rows_guard = self.rows.read().unwrap();
        let index_guard = self.index.read().unwrap();

        let row_ids = index_guard.get(key);
        row_ids
            .iter()
            .filter_map(|id| {
                let row = rows_guard.get(id);
                if let Some(value) = row {
                    let value_clone = value.clone();
                    return Some(Indexed::new(*id, value_clone));
                }
                None
            })
            .collect()
    }

    pub fn get_values(&self, key: &KeyT) -> Vec<ValueT> {
        let indexed = self.get(key);
        indexed.into_iter().map(|i| i.value().clone()).collect()
    }
}

pub struct IndexWrite<KeyT, ValueT> {
    index: Arc<RwLock<Index<KeyT, ValueT>>>,
}

impl<KeyT: PartialEq + Eq + Hash, ValueT> IndexWrite<KeyT, ValueT> {
    pub fn new(index: Arc<RwLock<Index<KeyT, ValueT>>>) -> Self {
        IndexWrite { index }
    }
}

impl<KeyT: PartialEq + Eq + Hash, ValueT> Indexable<ValueT> for IndexWrite<KeyT, ValueT> {
    fn insert(&mut self, row: Indexed<ValueT>) -> IndexId {
        self.index.write().unwrap().insert(row)
    }

    fn delete(&mut self, row: Indexed<ValueT>) {
        self.index.write().unwrap().delete(row)
    }
}
