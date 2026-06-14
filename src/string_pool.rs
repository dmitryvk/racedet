use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

#[derive(Debug)]
pub(crate) struct StringPool {
    inner: RwLock<Inner>,
}

#[derive(Debug)]
struct Inner {
    vec: Vec<Arc<str>>,
    map: HashMap<Arc<str>, StringIdx>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct StringIdx(usize);

impl StringIdx {
    pub(crate) fn as_usize(self) -> usize {
        self.0
    }
}

impl StringPool {
    pub(crate) fn new() -> Self {
        Self {
            inner: RwLock::new(Inner {
                vec: Vec::new(),
                map: HashMap::new(),
            }),
        }
    }

    pub(crate) fn intern(&self, value: &str) -> StringIdx {
        let mut inner = self
            .inner
            .write()
            .expect("not panicking while holding the lock");
        if let Some(idx) = inner.map.get(value) {
            return *idx;
        };
        let idx = StringIdx(inner.vec.len() + 1);
        let value = Arc::<str>::from(value);
        inner.vec.push(value.clone());
        inner.map.insert(value, idx);
        idx
    }

    pub(crate) fn get(&self, idx: StringIdx) -> Option<Arc<str>> {
        let inner = self
            .inner
            .read()
            .expect("not panicking while holding the lock");
        inner.vec.get(idx.0 - 1).cloned()
    }

    pub(crate) fn iter_ordered(&self) -> impl Iterator<Item = (StringIdx, Arc<str>)> {
        let mut cur = StringIdx(1);
        std::iter::from_fn(move || {
            let item = self.get(cur).map(|s| (cur, s));
            cur = StringIdx(cur.0 + 1);
            item
        })
    }
}

impl std::fmt::Display for StringIdx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
