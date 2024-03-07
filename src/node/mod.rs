use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::datastore::*;
use crate::durability::omnipaxos_durability::Log;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use omnipaxos::messages::{self, *};
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::hash::Hash;
use std::iter;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time;

use std::time::Duration;

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);

pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    pub incoming: mpsc::Receiver<Message<Log>>,
    pub outgoing: HashMap<NodeId, mpsc::Sender<Message<Log>>>,
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self
            .node
            .lock()
            .unwrap()
            .omni_paxos_durability
            .omni_paxos
            .outgoing_messages();
        for msg in messages {
            let receiver = msg.get_receiver();
            let channel = self
                .outgoing
                .get_mut(&receiver)
                .expect("No channel for receiver");
            let _ = channel.send(msg).await;
        }
    }

    pub async fn run(&mut self) {
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        loop {
            tokio::select! {
                biased;

                _ = tick_interval.tick() => {
                    self.node.lock().unwrap().omni_paxos_durability.omni_paxos.tick();
                }
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs().await;
                }
                Some(in_msg) = self.incoming.recv() => {
                    self.node.lock().unwrap().omni_paxos_durability.omni_paxos.handle_incoming(in_msg);
                }
                else => { }
            }
        }
    }
}

pub struct Node {
    node_id: NodeId, // Unique identifier for the node
    // TODO Datastore and OmniPaxosDurability
    omni_paxos_durability: OmniPaxosDurability,
    data_store: ExampleDatastore,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        let omni_paxos_durability = omni_durability;
        let data_store = ExampleDatastore::new();
        Node {
            node_id,
            omni_paxos_durability,
            data_store,
        }
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        let curr_leader = self.omni_paxos_durability.omni_paxos.get_current_leader();
        if curr_leader == Some(self.node_id) {
            self.apply_replicated_txns();
        } else {
            self.data_store
                .rollback_to_replicated_durability_offset()
                .expect("Failed to rollback");
        }
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        let durable_tx_offset = self.omni_paxos_durability.get_durable_tx_offset();
        let replicated_txns = self.data_store.get_replicated_offset();
        let leader = self.omni_paxos_durability.omni_paxos.get_current_leader();

        // we need to advance the durable offset before we replay the transactions
        self.advance_replicated_durability_offset()
            .expect("Failed to advance durable offset");

        if replicated_txns < Some(durable_tx_offset) && leader != Some(self.node_id) {
            // println!(
            //     "Replicated txns: {:?}, Durable tx offset: {:?}",
            //     replicated_txns, durable_tx_offset
            // );

            println!(
                "Current leader: {:?}, Current nodeId {:?}",
                leader, self.node_id
            );

            let iter: Box<dyn Iterator<Item = (TxOffset, tx_data::TxData)>>;

            // check if replicated_txns is None
            if replicated_txns.is_none() {
                iter = self.omni_paxos_durability.iter();
            } else {
                iter = self
                    .omni_paxos_durability
                    .iter_starting_from_offset(replicated_txns.unwrap());
            }

            for entry in iter {
                println!("Replaying transactions: {:?}", entry);
                println!("Data store: {:?}", self.data_store.get_cur_offset());
                self.data_store
                    .replay_transaction(&entry.1)
                    .expect("Failed to replay transaction");
            }
        }
    }

    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        self.data_store.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        self.data_store.release_tx(tx)
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        let leader = self.omni_paxos_durability.omni_paxos.get_current_leader();
        if leader == Some(self.node_id) {
            Ok(self.data_store.begin_mut_tx())
        } else {
            Err(DatastoreError::NotLeader)
        }
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        let leader = self.omni_paxos_durability.omni_paxos.get_current_leader();
        if leader == Some(self.node_id) {
            let tx_result = self.data_store.commit_mut_tx(tx);
            tx_result
        } else {
            Err(DatastoreError::NotLeader)
        }
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        let durable_tx_offset = self.omni_paxos_durability.get_durable_tx_offset();
        self.data_store
            .advance_replicated_durability_offset(durable_tx_offset)
    }
}

/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
///
/// A few helper functions to help structure your tests have been defined that you are welcome to use.
#[cfg(test)]
mod tests {
    use crate::durability::omnipaxos_durability::Log;
    use crate::durability::omnipaxos_durability::OmniPaxosDurability;
    // use crate::node::tx_data::DeleteList;
    // use crate::node::tx_data::InsertList;
    // use crate::node::tx_data::RowData;
    // use crate::node::tx_data::TxData;
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::{LogEntry, NodeId};
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use std::collections::HashMap;
    // use std::sync::mpsc::Receiver;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    const SERVERS: [NodeId; 3] = [1, 2, 3];

    #[allow(clippy::type_complexity)]
    fn initialise_channels() -> (
        HashMap<NodeId, mpsc::Sender<Message<Log>>>,
        HashMap<NodeId, mpsc::Receiver<Message<Log>>>,
    ) {
        let mut sender_channels: HashMap<u64, mpsc::Sender<Message<Log>>> = HashMap::new();
        let mut receiver_channels: HashMap<u64, mpsc::Receiver<Message<Log>>> = HashMap::new();

        for server_id in SERVERS {
            let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
            sender_channels.insert(server_id, sender);
            receiver_channels.insert(server_id, receiver);
        }
        (sender_channels, receiver_channels)
    }

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    /// generate multiple nodes and store them in a HashMap, each node consisting of an Arc<Mutex<Node>> and a JoinHandle<()>.
    fn spawn_nodes(runtime: &mut Runtime) -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        let config_id = 1;
        let mut op_server_handle = HashMap::new();
        let (sender_channels, mut receiver_channels) = initialise_channels();

        for server_id in SERVERS {
            let server_config = ServerConfig {
                pid: server_id,
                election_tick_timeout: ELECTION_TICK_TIMEOUT,
                ..Default::default()
            };
            let cluster_config = ClusterConfig {
                configuration_id: config_id,
                nodes: SERVERS.into(),
                ..Default::default()
            };
            let op_config = OmniPaxosConfig {
                server_config,
                cluster_config,
            };
            let node: Arc<Mutex<Node>> = Arc::new(Mutex::new(
                Node::new(
                    server_id,
                    OmniPaxosDurability {
                        omni_paxos: op_config.build(MemoryStorage::default()).unwrap(),
                    },
                ), // op_config.build(MemoryStorage::default()).unwrap(),
            ));
            let mut op_server = NodeRunner {
                node: Arc::clone(&node),
                incoming: receiver_channels.remove(&server_id).unwrap(),
                outgoing: sender_channels.clone(),
            };
            let join_handle: JoinHandle<()> = runtime.spawn({
                async move {
                    op_server.run().await;
                }
            });
            op_server_handle.insert(server_id, (node, join_handle));
        }

        // ********************************************************************************************************************
        // println!("Killing leader: {}...", leader);
        // leader_join_handle.abort();
        // // wait for new leader to be elected...
        // std::thread::sleep(WAIT_LEADER_TIMEOUT);

        // let leader = follower_servers
        //     .lock()
        //     .unwrap()
        //     .omni_paxos_durability
        //     .omni_paxos
        //     .get_current_leader()
        //     .expect("No leader elected");
        // println!("New leader elected: {}", leader);

        // let (leader_server, _) = op_server_handle.get(&leader).unwrap();

        // // update the leader and follower servers after the leader has been killed
        // for server in op_server_handle.values() {
        //     println!(
        //         "Applying replicated txns: {:?}",
        //         server.0.lock().unwrap().node_id
        //     );
        //     server.0.lock().unwrap().update_leader();
        // }

        // let mut tx3 = leader_server
        //     .lock()
        //     .unwrap()
        //     .begin_mut_tx()
        //     .expect("Failed to begin mutable transaction");

        // tx3.set("duc".to_string(), "pham".to_string());

        // println!(
        //     "Committing mutable transaction: {:?}",
        //     tx3.get(&"duc".to_string())
        // );
        // // begin a mutable transaction
        // let result3 = leader_server.lock().unwrap().commit_mut_tx(tx3).unwrap();
        // // let (leader_server, _) = op_server_handle.get(&leader).unwrap();
        // leader_server
        //     .lock()
        //     .unwrap()
        //     .omni_paxos_durability
        //     .append_tx(result3.tx_offset, result3.tx_data);

        // std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // // apply the committed transactions to the follower servers and advance the replicated offset to all nodes
        // for server in op_server_handle.values() {
        //     println!(
        //         "Applying replicated txns: {:?}",
        //         server.0.lock().unwrap().node_id
        //     );
        //     server.0.lock().unwrap().apply_replicated_txns();
        // }

        // let committed_ents = follower_servers
        //     .lock()
        //     .unwrap()
        //     .omni_paxos_durability
        //     .omni_paxos
        //     .read_decided_suffix(2)
        //     .expect("Failed to read from OmniPaxos log");

        // for ent in committed_ents {
        //     if let LogEntry::Decided(log) = ent {
        //         println!("Adding to simple log store: {:?}", log);
        //     }
        //     // ignore uncommitted entries
        // }

        op_server_handle
    }

    /// Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
    #[test]
    fn test_1() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        assert_eq!(nodes.len(), 3);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (first_server, _) = nodes.get(&1).unwrap();

        let leader = first_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .omni_paxos
            .get_current_leader()
            .expect("No leader elected");

        println!("Leader elected: {}", leader);

        let follower = SERVERS.iter().find(|&&x| x != leader).unwrap();
        println!("Follower: {}", follower);
        // let (follower_servers, _) = nodes.get(follower).unwrap();

        let (leader_server, _) = nodes.get(&leader).unwrap();

        // begin a mutable transaction
        let mut tx1 = leader_server
            .lock()
            .unwrap()
            .begin_mut_tx()
            .expect("Failed to begin mutable transaction");

        tx1.set("foo".to_string(), "bar".to_string());

        println!(
            "Committing mutable transaction: {:?}",
            tx1.get(&"foo".to_string())
        );
        let result1 = leader_server.lock().unwrap().commit_mut_tx(tx1).unwrap();

        // append a transaction to the OmniPaxos log
        leader_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .append_tx(result1.tx_offset, result1.tx_data);

        // check that replicated offset is the same for all nodes
        for server in nodes.values() {
            let replicated_tx = server.0.lock().unwrap().data_store.get_replicated_offset();

            println!("Replicated offset: {:?}", replicated_tx);
        }

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes
        for server in nodes.values() {
            println!(
                "Applying replicated txns: {:?}",
                server.0.lock().unwrap().node_id
            );
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        for server in nodes.values() {
            let tx = &server
                .0
                .lock()
                .unwrap()
                .data_store
                .begin_tx(DurabilityLevel::Replicated);
            let value = tx.get(&"foo".to_string());
            println!("Data store: {:?}", value);
        }

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // check that replicated offset is the same for all nodes
        for server in nodes.values() {
            let replicated_tx = server.0.lock().unwrap().data_store.get_replicated_offset();

            println!("Replicated offset: {:?}", replicated_tx);
        }

        // check that the committed transactions are the same for all nodes
        for server in nodes.values() {
            let committed_ents = server
                .0
                .lock()
                .unwrap()
                .omni_paxos_durability
                .omni_paxos
                .read_decided_suffix(0)
                .expect("Failed to read from OmniPaxos log");

            for ent in committed_ents {
                if let LogEntry::Decided(log) = ent {
                    println!("Decided log: {:?}", log);
                }
                // ignore uncommitted entries
            }
        }
    }
}
