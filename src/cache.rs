use slab::Slab;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::{HashMap, DefaultHasher};
use std::sync::{Arc, Mutex};
use std::fmt;

#[derive(Debug)]
struct Entry<K, V> {
    key: K,
    value: V,
}

#[derive(Debug)]
enum Bucket<K, V> {
    Empty,
    Full(Entry<K, V>, Link<K, V>),
}

impl<K: Hash + Eq + fmt::Debug, V: Clone + fmt::Debug> Bucket<K, V> {
    fn insert(&mut self, entry: Entry<K, V>) {
        match self {
            &mut Bucket::Empty => *self = Bucket::Full(entry, None),
            &mut Bucket::Full(_, Some(ref mut link)) => link.insert(entry),
            &mut Bucket::Full(_, ref mut l) => {
                *l = Some(Box::new(Bucket::Full(entry, None)));
            }
        }
    }
    fn get(&self, key: K) -> Option<&V> {
        match *self {
            Bucket::Empty => None,
            Bucket::Full(ref entry, ref next) => {
                if entry.key == key {
                    return Some(&entry.value);
                }
                next.as_ref().and_then(|next| next.get(key))
            }
        }
    }
}
type Link<K, V> = Option<Box<Bucket<K, V>>>;

#[derive(Debug)]
struct Partition<K, V> {
    indexes: HashMap<u64, usize>,
    slab: Slab<Bucket<K, V>>,
}

impl<K: Hash + Eq + fmt::Debug, V: Clone + fmt::Debug> Partition<K, V> {
    fn with_capacity(cap: usize) -> Partition<K, V> {
        Partition {
            slab: Slab::with_capacity(cap),
            indexes: HashMap::new(),
        }
    }

    fn insert(&mut self, hash: u64, entry: Entry<K, V>) {
        let index = self.indexes.get(&hash).cloned();
        match index {
            None => {
                self.slab.insert(Bucket::Full(entry, None)).map(|idx| {
                    self.indexes.insert(hash, idx)
                });
            }

            Some(idx) => self.slab.get_mut(idx).unwrap().insert(entry),
        }
    }

    fn get(&self, hash: u64, key: K) -> Option<V> {
        let index = self.indexes.get(&hash).cloned();
        match index {
            Some(index) => {
                let bucket = self.slab.get(index).unwrap();
                bucket.get(key).cloned()
            }
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct Cache<K, V> {
    partition_count: usize,
    partitions: Vec<Arc<Mutex<Partition<K, V>>>>,
}

#[derive(Debug)]
pub struct CacheHandle<K, V> {
    partition_count: usize,
    cache: *mut Cache<K, V>,
}

impl<K: Hash + Eq + fmt::Debug, V: Clone + fmt::Debug> CacheHandle<K, V> {
    pub fn get<'a>(&mut self, key: K) -> Option<V> {
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let slot = hash % self.partition_count as u64;
        let mut rv = None;
        unsafe {
            if !self.cache.is_null() {
                let mut partition = (*self.cache).get_partition(slot as usize);
                partition.lock().map(|partition| {
                    rv = partition.get(hash, key);
                });
            }
        }
        rv
    }

    pub fn insert(&mut self, key: K, value: V) {
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let slot = hash % self.partition_count as u64;
        let entry = Entry {
            key: key,
            value: value,
        };
        unsafe {
            if !self.cache.is_null() {
                let partition = (*self.cache).get_partition(slot as usize);
                partition.lock().map(|mut partition| {
                    partition.insert(hash, entry)
                });
            }
        }
    }
}

impl<K: Hash + Eq + fmt::Debug, V: Clone + fmt::Debug> Cache<K, V> {
    fn with_partitions(count: usize) -> Cache<K, V> {
        let mut partitions = Vec::with_capacity(count);

        for _ in 0..count {
            partitions.push(Arc::new(Mutex::new(Partition::with_capacity(100))));
        }

        Cache {
            partition_count: count,
            partitions: partitions,
        }
    }

    fn get_partition(&self, slot: usize) -> Arc<Mutex<Partition<K, V>>> {
        self.partitions[slot].clone()
    }

    fn handle(&mut self) -> CacheHandle<K, V> {
        let ptr: *mut Cache<K, V> = self;
        CacheHandle {
            cache: ptr,
            partition_count: self.partition_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test;
    use rand::Rng;
    use rand::thread_rng;

    fn build_kvs(count: usize) -> Vec<([u8; 12], [u8; 12])> {
        let mut rng = thread_rng();
        let mut kvs = vec![];
        for i in 0..count {
            let mut key: [u8; 12] = [0; 12];
            let mut value: [u8; 12] = [0; 12];
            rng.fill_bytes(&mut key);
            rng.fill_bytes(&mut value);
            kvs.push((key, value));
        }
        kvs
    }

    #[test]
    fn test_1() {
        let mut c = Cache::with_partitions(2);
        let mut handle = c.handle();

        let tests = build_kvs(150);

        for &(key, value) in tests.iter() {
            handle.insert(key, value)
        }

        for &(key, value) in tests.iter() {
            assert_eq!(handle.get(key), Some(value));
        }

        println!("cache: {:?}", c);
    }

    #[bench]
    fn bench_1(b: &mut test::Bencher) {
        let tests = build_kvs(100);
        b.iter(|| {
            let mut c = Cache::with_partitions(2);
            let mut handle = c.handle();
            let tests = build_kvs(100);

            for &(key, value) in tests.iter() {
                handle.insert(key, value)
            }

            for &(key, value) in tests.iter() {
                assert_eq!(handle.get(key), Some(value));
            }
        });
    }
}
