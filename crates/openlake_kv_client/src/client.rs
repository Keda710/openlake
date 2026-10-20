use std::sync::Mutex;

use openlake_io::rpc::{CLIENT_NODE_ID_BASE, CLIENT_NODE_ID_MAX};
use xxhash_rust::xxh64::xxh64;

use crate::transport::{Protocol, Scatter, Waiter};

pub trait KvClient: Send + Sync {
    fn attach(&self, addr: &str, node_id: u16, slot_bytes: u32) -> Result<usize, String>;
    fn register_memory(&self, addr: u64, len: u64) -> Result<(), String>;
    fn batch_is_exist(&self, keys: &[Vec<u8>]) -> Result<Vec<i32>, String>;
    fn put_batch(
        &self,
        keys: &[Vec<u8>],
        addrs: &[Vec<u64>],
        sizes: &[Vec<u64>],
    ) -> Result<Vec<i32>, String>;
    fn get_batch(
        &self,
        keys: &[Vec<u8>],
        addrs: &[Vec<u64>],
        sizes: &[Vec<u64>],
    ) -> Result<Vec<i32>, String>;
    fn reset(&self) -> Result<(), String>;
    fn close(&mut self);
    fn client_id(&self) -> u16;
}

pub struct StoreClient {
    proto: Box<dyn Protocol>,
    nodes: Mutex<Vec<u16>>,
    client_id: u16,
}

impl StoreClient {
    pub fn new(proto: Box<dyn Protocol>, client_id: u16) -> Result<Self, String> {
        if !(CLIENT_NODE_ID_BASE..=CLIENT_NODE_ID_MAX).contains(&client_id) {
            return Err(format!(
                "client_id {client_id} outside [{CLIENT_NODE_ID_BASE}, {CLIENT_NODE_ID_MAX}]"
            ));
        }
        Ok(Self {
            proto,
            nodes: Mutex::new(Vec::new()),
            client_id,
        })
    }

    fn owners(&self, keys: &[Vec<u8>]) -> Result<(Vec<u16>, Vec<Vec<usize>>), String> {
        let nodes = self.nodes.lock().unwrap();
        if nodes.is_empty() {
            return Err("no nodes attached".into());
        }
        let mut groups: Vec<Vec<usize>> = vec![Vec::new(); nodes.len()];
        for (i, key) in keys.iter().enumerate() {
            groups[(xxh64(key, 0) % nodes.len() as u64) as usize].push(i);
        }
        Ok((nodes.clone(), groups))
    }

    fn sharded<F>(
        &self,
        keys: &[Vec<u8>],
        scatters: Option<&[Scatter]>,
        op: F,
    ) -> Result<Vec<i32>, String>
    where
        F: Fn(&dyn Protocol, u16, &[Vec<u8>], &[Scatter]) -> Result<Waiter, String>,
    {
        let (nodes, groups) = self.owners(keys)?;
        let mut out = vec![0i32; keys.len()];
        let mut pending: Vec<(u16, &Vec<usize>, Waiter)> = Vec::with_capacity(groups.len());
        let mut first_err = None;
        for (g, idxs) in groups.iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let node_keys: Vec<Vec<u8>> = idxs.iter().map(|&i| keys[i].clone()).collect();
            let node_scatters: Vec<Scatter> = match scatters {
                Some(s) => idxs.iter().map(|&i| s[i].clone()).collect(),
                None => Vec::new(),
            };
            match op(self.proto.as_ref(), nodes[g], &node_keys, &node_scatters) {
                Ok(wait) => pending.push((nodes[g], idxs, wait)),
                Err(error) => {
                    first_err = Some(error);
                    break;
                }
            }
        }
        for (node, idxs, wait) in pending {
            match wait.recv() {
                Ok(Ok(results)) if results.len() == idxs.len() => {
                    for (&i, r) in idxs.iter().zip(results) {
                        out[i] = r;
                    }
                }
                Ok(Ok(results)) => {
                    first_err.get_or_insert(format!(
                        "node {node}: {} results for {} keys",
                        results.len(),
                        idxs.len()
                    ));
                }
                Ok(Err(e)) => {
                    first_err.get_or_insert(e);
                }
                Err(_) => {
                    first_err.get_or_insert_with(|| format!("node {node}: client thread died"));
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(out),
        }
    }

    fn zip_scatters(addrs: &[Vec<u64>], sizes: &[Vec<u64>]) -> Vec<Scatter> {
        addrs
            .iter()
            .zip(sizes)
            .map(|(a, s)| a.iter().copied().zip(s.iter().copied()).collect())
            .collect()
    }

    fn validate(keys: &[Vec<u8>], addrs: &[Vec<u64>], sizes: &[Vec<u64>]) -> Result<(), String> {
        if keys.len() != addrs.len() || addrs.len() != sizes.len() {
            return Err(format!(
                "mismatched batch: {} keys, {} addr lists, {} size lists",
                keys.len(),
                addrs.len(),
                sizes.len()
            ));
        }
        for (i, (a, s)) in addrs.iter().zip(sizes).enumerate() {
            if a.len() != s.len() {
                return Err(format!("key {i}: {} addrs but {} sizes", a.len(), s.len()));
            }
        }
        Ok(())
    }
}

impl KvClient for StoreClient {
    fn attach(&self, addr: &str, node_id: u16, slot_bytes: u32) -> Result<usize, String> {
        let n = self.proto.attach(addr, node_id, slot_bytes)?;
        let mut nodes = self.nodes.lock().unwrap();
        if !nodes.contains(&node_id) {
            nodes.push(node_id);
        }
        Ok(n)
    }

    fn register_memory(&self, addr: u64, len: u64) -> Result<(), String> {
        self.proto.register_memory(addr, len)
    }

    fn batch_is_exist(&self, keys: &[Vec<u8>]) -> Result<Vec<i32>, String> {
        self.sharded(keys, None, |p, node, keys, _| p.exists(node, keys))
    }

    fn put_batch(
        &self,
        keys: &[Vec<u8>],
        addrs: &[Vec<u64>],
        sizes: &[Vec<u64>],
    ) -> Result<Vec<i32>, String> {
        Self::validate(keys, addrs, sizes)?;
        let scatters = Self::zip_scatters(addrs, sizes);
        self.sharded(keys, Some(&scatters), |p, node, keys, sc| {
            p.put(node, keys, sc)
        })
    }

    fn get_batch(
        &self,
        keys: &[Vec<u8>],
        addrs: &[Vec<u64>],
        sizes: &[Vec<u64>],
    ) -> Result<Vec<i32>, String> {
        Self::validate(keys, addrs, sizes)?;
        let scatters = Self::zip_scatters(addrs, sizes);
        self.sharded(keys, Some(&scatters), |p, node, keys, sc| {
            p.get(node, keys, sc)
        })
    }

    fn reset(&self) -> Result<(), String> {
        let nodes = self.nodes.lock().unwrap().clone();
        for node in nodes {
            self.proto.reset(node)?;
        }
        Ok(())
    }

    fn close(&mut self) {
        self.proto.close();
    }

    fn client_id(&self) -> u16 {
        self.client_id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{mpsc, Mutex};
    use std::time::Duration;

    use super::*;

    struct MockProtocol {
        actions: Mutex<HashMap<u16, Result<Waiter, String>>>,
        dispatched: mpsc::Sender<u16>,
    }

    impl MockProtocol {
        fn dispatch(&self, node: u16) -> Result<Waiter, String> {
            self.dispatched.send(node).unwrap();
            self.actions.lock().unwrap().remove(&node).unwrap()
        }
    }

    impl Protocol for MockProtocol {
        fn attach(&self, _addr: &str, _node: u16, _slot_bytes: u32) -> Result<usize, String> {
            Ok(1)
        }

        fn register_memory(&self, _addr: u64, _len: u64) -> Result<(), String> {
            Ok(())
        }

        fn exists(&self, node: u16, _keys: &[Vec<u8>]) -> Result<Waiter, String> {
            self.dispatch(node)
        }

        fn put(
            &self,
            node: u16,
            _keys: &[Vec<u8>],
            _scatters: &[Scatter],
        ) -> Result<Waiter, String> {
            self.dispatch(node)
        }

        fn get(
            &self,
            node: u16,
            _keys: &[Vec<u8>],
            _scatters: &[Scatter],
        ) -> Result<Waiter, String> {
            self.dispatch(node)
        }

        fn reset(&self, _node: u16) -> Result<(), String> {
            Ok(())
        }

        fn close(&mut self) {}
    }

    fn keys_for_nodes(count: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|owner| {
                (0u32..)
                    .find(|candidate| {
                        xxh64(&candidate.to_le_bytes(), 0) % count as u64 == owner as u64
                    })
                    .unwrap()
                    .to_le_bytes()
                    .to_vec()
            })
            .collect()
    }

    fn client_with_actions(
        actions: HashMap<u16, Result<Waiter, String>>,
        dispatched: mpsc::Sender<u16>,
        count: usize,
    ) -> StoreClient {
        let client = StoreClient::new(
            Box::new(MockProtocol {
                actions: Mutex::new(actions),
                dispatched,
            }),
            CLIENT_NODE_ID_BASE,
        )
        .unwrap();
        for node in 0..count as u16 {
            client.attach("unused", node, 0).unwrap();
        }
        client
    }

    #[test]
    fn put_dispatch_failure_waits_for_accepted_shard_and_stops_dispatch() {
        let (complete_tx, complete_rx) = mpsc::channel();
        let (dispatched_tx, dispatched_rx) = mpsc::channel();
        let actions = HashMap::from([(0, Ok(complete_rx)), (1, Err("dispatch failed".to_owned()))]);
        let client = client_with_actions(actions, dispatched_tx, 3);
        let keys = keys_for_nodes(3);
        let addrs = vec![vec![0]; 3];
        let sizes = vec![vec![1]; 3];
        let (result_tx, result_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            result_tx
                .send(client.put_batch(&keys, &addrs, &sizes))
                .unwrap();
        });

        assert_eq!(
            dispatched_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            0
        );
        assert_eq!(
            dispatched_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            1
        );
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        complete_tx.send(Ok(vec![1])).unwrap();
        assert_eq!(
            result_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Err("dispatch failed".to_owned())
        );
        assert!(matches!(
            dispatched_rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
        worker.join().unwrap();
    }

    #[test]
    fn get_disconnected_completion_waits_for_other_shards() {
        let (dropped_tx, dropped_rx) = mpsc::channel();
        drop(dropped_tx);
        let (complete_tx, complete_rx) = mpsc::channel();
        let (dispatched_tx, dispatched_rx) = mpsc::channel();
        let actions = HashMap::from([(0, Ok(dropped_rx)), (1, Ok(complete_rx))]);
        let client = client_with_actions(actions, dispatched_tx, 2);
        let keys = keys_for_nodes(2);
        let addrs = vec![vec![0]; 2];
        let sizes = vec![vec![1]; 2];
        let (result_tx, result_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            result_tx
                .send(client.get_batch(&keys, &addrs, &sizes))
                .unwrap();
        });

        assert_eq!(
            dispatched_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            0
        );
        assert_eq!(
            dispatched_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            1
        );
        assert!(matches!(
            result_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        complete_tx.send(Ok(vec![1])).unwrap();
        let error = result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(error.contains("node 0: client thread died"));
        worker.join().unwrap();
    }

    #[test]
    fn successful_shards_preserve_input_order() {
        let (dispatched_tx, dispatched_rx) = mpsc::channel();
        let actions = (0..3)
            .map(|node| {
                let (complete_tx, complete_rx) = mpsc::channel();
                complete_tx.send(Ok(vec![node as i32 + 10])).unwrap();
                (node, Ok(complete_rx))
            })
            .collect();
        let client = client_with_actions(actions, dispatched_tx, 3);
        let mut keys = keys_for_nodes(3);
        keys.reverse();

        assert_eq!(client.batch_is_exist(&keys), Ok(vec![12, 11, 10]));
        assert_eq!(dispatched_rx.try_iter().collect::<Vec<_>>(), [0, 1, 2]);
    }
}
