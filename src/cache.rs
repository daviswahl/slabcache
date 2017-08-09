use slab::Slab;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::{HashMap, DefaultHasher};
use std::sync::{Arc, Mutex};
use std::fmt;

#[derive(Debug, Clone)]
struct Entry<K, V> {
    key: K,
    value: V,
}

#[derive(Debug)]
enum Bucket<K, V> {
    Empty,
    Full(Entry<K, V>, Link<K, V>),
}

impl<K: Hash + Eq + fmt::Debug + Clone, V: Clone + fmt::Debug> Bucket<K, V> {
    fn insert(&mut self, entry: Entry<K, V>) {
        match self {
            &mut Bucket::Empty => *self = Bucket::Full(entry, None),
            &mut Bucket::Full(_, Some(ref mut link)) => link.insert(entry),
            &mut Bucket::Full(_, ref mut l) => {
                *l = Some(Box::new(Bucket::Full(entry, None)));
            }
        }
    }

    fn get(&self, key: K) -> Option<Entry<K, V>> {
        match *self {
            Bucket::Empty => None,
            Bucket::Full(ref entry, ref next) => {
                if entry.key == key {
                    return Some(entry.clone());
                }
                next.as_ref().and_then(|next| next.get(key))
            }
        }
    }
}
type Link<K, V> = Option<Box<Bucket<K, V>>>;

struct Partition<K, V> {
    indexes: HashMap<u64, usize>,
    slab: Slab<Bucket<K, V>>,
}

impl <K, V> fmt::Debug for Partition<K,V> where K: fmt::Debug, V: fmt::Debug {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
       write!(f, "{:?}", self.slab)
    }
}
impl<K: Hash + Eq + fmt::Debug + Clone, V: Clone + fmt::Debug> Partition<K, V> {
    fn with_capacity(cap: usize) -> Partition<K, V> {
        Partition {
            slab: Slab::with_capacity(cap),
            indexes: HashMap::with_capacity(cap),
        }
    }

    fn insert(&mut self, hash: u64, entry: Entry<K, V>) -> Result<(), Entry<K, V>> {
        let index = self.indexes.get(&hash).cloned();
        match index {
            None => {
                self.slab
                    .insert(Bucket::Full(entry, None))
                    .map(|idx| { self.indexes.insert(hash, idx); })
                    .map_err(|bucket| match bucket {
                        Bucket::Empty => unreachable!(),
                        Bucket::Full(e, _) => e,
                    })
            }

            Some(idx) => Ok(self.slab.get_mut(idx).unwrap().insert(entry)),
        }
    }

    fn get(&self, hash: u64, key: K) -> Option<V> {
        let index = self.indexes.get(&hash).cloned();
        match index {
            Some(index) => {
                let bucket = self.slab.get(index).unwrap();
                bucket.get(key).map(|entry| entry.value)
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

unsafe impl <K,V>Send for CacheHandle<K,V>{}

impl<K: Hash + Eq + fmt::Debug + Clone, V: Clone + fmt::Debug> CacheHandle<K, V> {
    pub fn get<'a>(&mut self, key: K) -> Result<Option<V>, ()> {
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let slot = hash % self.partition_count as u64;

        let partition;
        if !self.cache.is_null() {
            unsafe {
                partition = (*self.cache).get_partition(slot as usize);
            }
            partition
                .lock()
                .map(|partition| partition.get(hash, key))
                .map_err(|_| ())
        } else {
            Err(())
        }
    }


    pub fn insert(&mut self, key: K, value: V) -> Result<(), V> {
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let slot = hash % self.partition_count as u64;
        let partition;

        if !self.cache.is_null() {
            unsafe {
                partition = (*self.cache).get_partition(slot as usize);
            }
            match partition.lock() {
                Ok(mut lock) => {
                    lock.insert(
                        hash,
                        Entry {
                            key: key,
                            value: value,
                        },
                    ).map_err(|e| e.value)
                }
                Err(_) => Err(value),
            }
        } else {
            Err(value)
        }
    }
}

impl<K: Hash + Eq + fmt::Debug + Clone, V: Clone + fmt::Debug> Cache<K, V> {
    pub fn new(capacity: usize, partition_count: usize) -> Cache<K, V> {
        let mut partitions = Vec::with_capacity(partition_count);

        for _ in 0..partition_count {
            partitions.push(Arc::new(Mutex::new(Partition::with_capacity(capacity / partition_count))));
        }

        Cache {
            partition_count: partition_count,
            partitions: partitions,
        }
    }

    fn get_partition(&self, slot: usize) -> Arc<Mutex<Partition<K, V>>> {
        self.partitions[slot].clone()
    }

    pub fn handle(&mut self) -> CacheHandle<K, V> {
        CacheHandle {
            cache: self,
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
        let mut c = Cache::new(160, 2);
        let mut handle = c.handle();

        let tests = build_kvs(150);

        for &(key, value) in tests.iter() {
            handle.insert(key, value).unwrap();
        }

        for &(key, value) in tests.iter() {
            assert_eq!(handle.get(key), Ok(Some(value)));
        }

        println!("cache: {:?}", c);
    }

    #[test]
    fn test_capacity() {
        let c: Cache<String, String> = Cache::new(100, 5);
        println!("c: {:?}", c)
    }
    #[bench]
    fn bench_1(b: &mut test::Bencher) {
        let tests = build_kvs(15000);
        b.iter(|| {
            let mut c = Cache::new(50000, 100);
            let mut handle = c.handle();

            for &(key, value) in tests.iter() {
                handle.insert(key, value);
            }

            for &(key, value) in tests.iter() {
                assert_eq!(handle.get(key), Ok(Some(value)));
            }
        });
    }

    #[bench]
    fn bench_threaded(b: &mut test::Bencher) {
        use std::thread;
        let num_threads = 10;
        let mut tests = vec![];
        for _ in 0..num_threads {
            tests.push(build_kvs(1500))
        }

        b.iter(|| {
            let mut c = Cache::new(50000, 100);

            let mut threads = Vec::with_capacity(num_threads);

            for test in tests.clone().into_iter() {
                let mut handle = c.handle();
                let thread = thread::spawn(move || {
                    for &(key, value) in test.iter() {
                        handle.insert(key, value);
                    }

                    for &(key, value) in test.iter() {
                        assert_eq!(handle.get(key), Ok(Some(value)));
                    }
                });

                threads.push(thread);
            }

            for thread in threads {
                thread.join();
            }
        });
    }
}
