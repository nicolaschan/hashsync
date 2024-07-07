use std::{cmp::max, hash::Hash, sync::Arc};

use dashmap::DashMap;

use crate::{
    id::{Indexed, RowId},
    index::{Index, IndexRead, Indexable},
};

pub struct HashSync<'a, RowT> {
    rows: Arc<DashMap<RowId, RowT>>,
    next_id: RowId,
    indexes: Vec<Box<dyn Indexable<RowT> + 'a>>,
}

impl<'a, RowT: Clone + 'a> Default for HashSync<'a, RowT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a, RowT: Clone + 'a> HashSync<'a, RowT> {
    pub fn new() -> Self {
        HashSync {
            rows: Arc::new(DashMap::default()),
            next_id: RowId::new(0),
            indexes: Vec::new(),
        }
    }

    pub fn keys(&self) -> Vec<RowId> {
        self.rows.iter().map(|r| *r.key()).collect()
    }

    pub fn by_id(&self, id: RowId) -> Option<RowT> {
        self.rows.get(&id).map(|r| r.value().clone())
    }

    pub fn by_id_indexed(&self, id: RowId) -> Option<Indexed<RowT>> {
        self.by_id(id).map(|row| Indexed::new(id, row))
    }

    pub fn insert(&mut self, row: RowT) -> RowId {
        let id = self.next_id;
        self.insert_at(id, row);
        self.next_id = self.next_id.next();
        id
    }

    fn insert_at(&mut self, id: RowId, row: RowT) {
        let indexed = Indexed::new(id, row);
        for index in self.indexes.iter_mut() {
            index.insert(&indexed);
        }
        self.rows.insert(id, indexed.into_value());
    }

    pub fn delete(&mut self, id: RowId) -> Option<RowT> {
        let row = self.rows.remove(&id);
        if let Some(row) = row {
            let indexed = Indexed::new(id, row.1);
            for index in self.indexes.iter_mut() {
                index.delete(&indexed);
            }
            return Some(indexed.into_value());
        }
        None
    }

    pub fn replace(&mut self, id: RowId, row: RowT) {
        // TODO: Lock write guard here to prevent race conditions with reads
        self.delete(id);
        self.insert_at(id, row);
        self.next_id = max(id.next(), self.next_id);
    }

    pub fn index<IndexKeyT, IndexFn>(&mut self, index_fn: IndexFn) -> IndexRead<IndexKeyT, RowT>
    where
        IndexFn: Fn(&RowT) -> IndexKeyT + 'static,
        IndexKeyT: PartialEq + Eq + Hash + 'a,
    {
        let index_many_fn = move |row: &RowT| vec![index_fn(row)];
        self.index_many(index_many_fn)
    }

    pub fn index_many<IndexKeyT, IndexFn>(
        &mut self,
        index_fn: IndexFn,
    ) -> IndexRead<IndexKeyT, RowT>
    where
        IndexFn: Fn(&RowT) -> Vec<IndexKeyT> + 'static,
        IndexKeyT: PartialEq + Eq + Hash + 'a,
    {
        let index_id_many_fn = move |indexed: &Indexed<RowT>| index_fn(indexed.value());
        self.index_id_many(index_id_many_fn)
    }

    pub fn index_id<IndexKeyT, IndexFn>(&mut self, index_fn: IndexFn) -> IndexRead<IndexKeyT, RowT>
    where
        IndexFn: Fn(&Indexed<RowT>) -> IndexKeyT + 'static,
        IndexKeyT: PartialEq + Eq + Hash + 'a,
    {
        let index_many_fn = move |indexed: &Indexed<RowT>| vec![index_fn(indexed)];
        self.index_id_many(index_many_fn)
    }

    pub fn index_id_many<IndexKeyT, IndexFn>(
        &mut self,
        index_fn: IndexFn,
    ) -> IndexRead<IndexKeyT, RowT>
    where
        IndexFn: Fn(&Indexed<RowT>) -> Vec<IndexKeyT> + 'static,
        IndexKeyT: PartialEq + Eq + Hash + 'a,
    {
        let mut index = Index::new(Box::new(index_fn));
        for row in self.rows.iter() {
            let indexed = Indexed::new(*row.key(), row.value().clone());
            index.insert(&indexed);
        }
        let (index_read, index_write) = index.into_read_write(self.rows.clone());
        self.indexes.push(Box::new(index_write));
        index_read
    }

    pub fn drop_indexes(self) -> Self {
        HashSync {
            rows: self.rows,
            next_id: self.next_id,
            indexes: Vec::new(),
        }
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
    fn index_id() {
        let mut hs = HashSync::new();
        let row_id = hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 4));
        let index = hs.index_id(|indexed| (indexed.id(), indexed.value().0));

        let rows = index.get_values(&(row_id, 1));
        assert_eq!(rows.len(), 1);
        assert!(rows.contains(&(1, 2)));
        let rows = index.get_values(&(row_id, 2));
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn keys() {
        let mut hs = HashSync::new();
        let row_to_delete = hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 4));

        let keys = hs.keys();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&RowId::new(0)));
        assert!(keys.contains(&RowId::new(1)));
        assert!(keys.contains(&RowId::new(2)));

        hs.delete(row_to_delete);
        assert!(!hs.keys().contains(&row_to_delete));
    }

    #[test]
    fn by_id() {
        let mut hs = HashSync::new();
        let row1 = hs.insert((1, 2));
        let row2 = hs.insert((1, 3));
        let row3 = hs.insert((3, 4));

        assert_eq!(hs.by_id(row1), Some((1, 2)));
        assert_eq!(hs.by_id(row2), Some((1, 3)));
        assert_eq!(hs.by_id(row3), Some((3, 4)));
    }

    #[test]
    fn by_id_indexed() {
        let mut hs = HashSync::new();
        let row1 = hs.insert((1, 2));
        let row2 = hs.insert((1, 3));
        let row3 = hs.insert((3, 4));

        assert_eq!(hs.by_id_indexed(row1), Some(Indexed::new(row1, (1, 2))));
        assert_eq!(hs.by_id_indexed(row2), Some(Indexed::new(row2, (1, 3))));
        assert_eq!(hs.by_id_indexed(row3), Some(Indexed::new(row3, (3, 4))));
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

    #[test]
    fn replace() {
        let mut hs = HashSync::new();
        let row_to_replace = hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 2));
        let index1 = hs.index(|&(a, _b)| a);
        let index2 = hs.index(|&(_a, b)| b);

        hs.replace(row_to_replace, (1, 4));

        let rows1 = index1.get_values(&1);
        assert_eq!(rows1.len(), 2);
        assert!(rows1.contains(&(1, 3)));
        assert!(rows1.contains(&(1, 4)));

        let rows1_keys = index1.get(&1).iter().map(|i| i.id()).collect::<Vec<_>>();
        assert_eq!(rows1_keys.len(), 2);
        assert!(rows1_keys.contains(&row_to_replace));
        assert!(rows1_keys.contains(&row_to_replace.next()));

        let rows2 = index2.get_values(&2);
        assert_eq!(rows2.len(), 1);
        assert!(rows2.contains(&(3, 2)));
    }

    #[test]
    fn index_many() {
        let mut hs = HashSync::new();
        hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 1));
        let index = hs.index_many(|&(a, b)| vec![a, b]);

        let rows1 = index.get_values(&1);
        assert_eq!(rows1.len(), 3);
        assert!(rows1.contains(&(1, 2)));
        assert!(rows1.contains(&(1, 3)));
        assert!(rows1.contains(&(3, 1)));

        let rows2 = index.get_values(&3);
        assert_eq!(rows2.len(), 2);
        assert!(rows2.contains(&(1, 3)));
        assert!(rows2.contains(&(3, 1)));
    }

    #[test]
    fn index_many_with_removal() {
        let mut hs = HashSync::new();
        let row_to_delete = hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 1));
        let index = hs.index_many(|&(a, b)| vec![a, b]);

        hs.delete(row_to_delete);

        let rows1 = index.get_values(&2);
        assert_eq!(rows1.len(), 0);

        let rows2 = index.get_values(&1);
        assert_eq!(rows2.len(), 2);
        assert!(rows2.contains(&(1, 3)));
        assert!(rows2.contains(&(3, 1)));
    }

    #[test]
    fn replace_increases_max_id() {
        let mut hs = HashSync::new();
        hs.replace(RowId::new(5), (1, 4));

        let row_id = hs.insert((1, 2));
        assert_eq!(row_id, RowId::new(6));
    }

    #[test]
    fn index_keys() {
        let mut hs = HashSync::new();
        hs.insert((1, 2));
        hs.insert((1, 3));
        hs.insert((3, 1));
        let index = hs.index(|&(a, _b)| a);

        let keys = index.keys();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains(&1));
        assert!(keys.contains(&3));
    }

    #[test]
    fn index_keys_after_removal() {
        let mut hs = HashSync::new();
        let row_to_delete = hs.insert((1, 2));
        hs.insert((3, 1));
        let index = hs.index(|&(a, _b)| a);

        hs.delete(row_to_delete);

        let keys = index.keys();
        assert_eq!(keys.len(), 1);
        assert!(keys.contains(&3));
    }

    #[test]
    fn drop_indexes() {
        let mut hs = HashSync::new();
        let id1 = hs.insert((1, 2));
        let id2 = hs.insert((1, 3));
        let id3 = hs.insert((3, 1));
        let _index = hs.index(|&(a, _b)| a);

        let hs = hs.drop_indexes();
        assert_eq!(hs.by_id(id1), Some((1, 2)));
        assert_eq!(hs.by_id(id2), Some((1, 3)));
        assert_eq!(hs.by_id(id3), Some((3, 1)));
    }
}
