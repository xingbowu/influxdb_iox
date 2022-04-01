use std::{
    any::Any,
    cmp::Ordering,
    fmt::Debug,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
};

use parking_lot::Mutex;

use super::CacheBackend;

#[derive(Debug)]
pub struct SharedBackend {
    inner_backend: Mutex<Box<dyn CacheBackend<K = SharedKey, V = SharedValue>>>,
    next_tag: AtomicU64,
}

impl SharedBackend {
    pub fn new(inner_backend: Box<dyn CacheBackend<K = SharedKey, V = SharedValue>>) -> Arc<Self> {
        Arc::new(Self {
            inner_backend: Mutex::new(inner_backend),
            next_tag: AtomicU64::new(0),
        })
    }

    pub fn user<K, V>(self: &Arc<Self>) -> SharedBackendUser<K, V>
    where
        K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
        V: Clone + Debug + Send + 'static,
    {
        let tag = Tag(self
            .next_tag
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst));

        SharedBackendUser {
            _k: PhantomData::default(),
            _v: PhantomData::default(),
            shared: Arc::clone(self),
            tag,
        }
    }
}

pub struct SharedBackendUser<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    // phantom data that is Send and Sync, see https://stackoverflow.com/a/50201389
    _k: PhantomData<fn() -> K>,
    _v: PhantomData<fn() -> V>,

    shared: Arc<SharedBackend>,
    tag: Tag,
}

impl<K, V> SharedBackendUser<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn key(&self, k: K) -> SharedKey {
        SharedKey {
            inner: Box::new(SharedKeyTyped { k, tag: self.tag }),
        }
    }

    fn value(&self, v: V) -> SharedValue {
        SharedValue {
            inner: Box::new(SharedValueTyped { v, tag: self.tag }),
        }
    }
}

impl<K, V> Debug for SharedBackendUser<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBackendUser")
            .field("shared", &self.shared)
            .field("tag", &self.tag)
            .finish_non_exhaustive()
    }
}

impl<K, V> CacheBackend for SharedBackendUser<K, V>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
    V: Clone + Debug + Send + 'static,
{
    type K = K;
    type V = V;

    fn get(&mut self, k: &Self::K) -> Option<Self::V> {
        let k = self.key(k.clone());

        match self.shared.inner_backend.lock().get(&k) {
            Some(v) => {
                assert_eq!(v.inner.tag(), self.tag);
                Some(
                    v.inner
                        .v_any()
                        .downcast_ref::<V>()
                        .expect("checked tag")
                        .clone(),
                )
            }
            None => None,
        }
    }

    fn set(&mut self, k: Self::K, v: Self::V) {
        let k = self.key(k);
        let v = self.value(v);

        self.shared.inner_backend.lock().set(k, v)
    }

    fn remove(&mut self, k: &Self::K) {
        let k = self.key(k.clone());

        self.shared.inner_backend.lock().remove(&k)
    }

    fn is_empty(&self) -> bool {
        self.shared.inner_backend.lock().is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Tag(u64);

trait SharedKeyInner: Debug + Send + 'static {
    fn tag(&self) -> Tag;
    fn k_any(&self) -> &dyn Any;
    fn clone(&self) -> Box<dyn SharedKeyInner>;
    fn eq(&self, other: &dyn SharedKeyInner) -> bool;
    fn hash(&self, state: &mut dyn Hasher);
    fn partial_cmp(&self, other: &dyn SharedKeyInner) -> Option<Ordering>;
}

#[derive(Debug)]
struct SharedKeyTyped<K>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
{
    k: K,
    tag: Tag,
}

impl<K> SharedKeyInner for SharedKeyTyped<K>
where
    K: Clone + Eq + Hash + Ord + Debug + Send + 'static,
{
    fn tag(&self) -> Tag {
        self.tag
    }

    fn k_any(&self) -> &dyn Any {
        &self.k as &dyn Any
    }

    fn clone(&self) -> Box<dyn SharedKeyInner> {
        Box::new(Self {
            k: self.k.clone(),
            tag: self.tag,
        })
    }

    fn eq(&self, other: &dyn SharedKeyInner) -> bool {
        if self.tag == other.tag() {
            let other_k = other.k_any().downcast_ref::<K>().expect("checked tag");
            &self.k == other_k
        } else {
            false
        }
    }

    fn hash(&self, mut state: &mut dyn Hasher) {
        self.k.hash(&mut state);
        self.tag.hash(&mut state);
    }

    fn partial_cmp(&self, other: &dyn SharedKeyInner) -> Option<Ordering> {
        match self.tag.cmp(&other.tag()) {
            Ordering::Less => Some(Ordering::Less),
            Ordering::Greater => Some(Ordering::Greater),
            Ordering::Equal => {
                let other_k = other.k_any().downcast_ref::<K>().expect("checked tag");
                Some(self.k.cmp(other_k))
            }
        }
    }
}

#[derive(Debug)]
pub struct SharedKey {
    inner: Box<dyn SharedKeyInner>,
}

impl Clone for SharedKey {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl PartialEq for SharedKey {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(other.inner.as_ref())
    }
}

impl Eq for SharedKey {}

impl Hash for SharedKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl PartialOrd for SharedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.partial_cmp(other.inner.as_ref())
    }
}

impl Ord for SharedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Implements Ord")
    }
}

trait SharedValueInner: Debug + Send + 'static {
    fn tag(&self) -> Tag;
    fn v_any(&self) -> &dyn Any;
    fn clone(&self) -> Box<dyn SharedValueInner>;
}

#[derive(Debug)]
struct SharedValueTyped<V>
where
    V: Clone + Debug + Send + 'static,
{
    v: V,
    tag: Tag,
}

impl<V> SharedValueInner for SharedValueTyped<V>
where
    V: Clone + Debug + Send + 'static,
{
    fn tag(&self) -> Tag {
        self.tag
    }

    fn v_any(&self) -> &dyn Any {
        &self.v as &dyn Any
    }

    fn clone(&self) -> Box<dyn SharedValueInner> {
        Box::new(Self {
            v: self.v.clone(),
            tag: self.tag,
        })
    }
}

#[derive(Debug)]
pub struct SharedValue {
    inner: Box<dyn SharedValueInner>,
}

impl Clone for SharedValue {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;

    #[test]
    fn test_shared_key() {
        let k1a = SharedKey {
            inner: Box::new(SharedKeyTyped {
                k: 42i8,
                tag: Tag(0),
            }),
        };
        let k1b = SharedKey {
            inner: Box::new(SharedKeyTyped {
                k: 42i8,
                tag: Tag(0),
            }),
        };
        let k1c = k1a.clone();

        let k2 = SharedKey {
            inner: Box::new(SharedKeyTyped {
                k: 41i8,
                tag: Tag(0),
            }),
        };
        let k3 = SharedKey {
            inner: Box::new(SharedKeyTyped {
                k: 42i8,
                tag: Tag(1),
            }),
        };
        let k4 = SharedKey {
            inner: Box::new(SharedKeyTyped {
                k: 42u8,
                tag: Tag(2),
            }),
        };

        assert_eq!(k1a, k1b);
        assert_eq!(k1a, k1c);
        assert_ne!(k1a, k2);
        assert_ne!(k1a, k3);
        assert_ne!(k1a, k4);

        assert_eq!(k1a.cmp(&k1b), Ordering::Equal);
        assert_eq!(k1a.cmp(&k1c), Ordering::Equal);
        assert_eq!(k1a.cmp(&k2), Ordering::Greater);
        assert_eq!(k1a.cmp(&k3), Ordering::Less);
        assert_eq!(k1a.cmp(&k4), Ordering::Less);
        assert_eq!(k2.cmp(&k3), Ordering::Less);

        assert_eq!(hash(&k1a), hash(&k1b));
        assert_eq!(hash(&k1a), hash(&k1c));
        assert_ne!(hash(&k1a), hash(&k2));
        assert_ne!(hash(&k1a), hash(&k3));
        assert_ne!(hash(&k1a), hash(&k4));
    }

    fn hash<T>(x: &T) -> u64
    where
        T: Hash,
    {
        let mut state = DefaultHasher::default();
        x.hash(&mut state);
        state.finish()
    }
}
