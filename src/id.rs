#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(usize);

impl RowId {
    pub fn new(id: usize) -> Self {
        RowId(id)
    }

    pub fn next(&self) -> Self {
        RowId(self.0 + 1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Indexed<T> {
    id: RowId,
    value: T,
}

impl<T> Indexed<T> {
    pub fn new(id: RowId, value: T) -> Self {
        Indexed { id, value }
    }

    pub fn id(&self) -> RowId {
        self.id
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub fn into_value(self) -> T {
        self.value
    }
}
