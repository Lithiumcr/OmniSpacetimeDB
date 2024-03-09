use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::datastore::*;
use crate::durability::omnipaxos_durability::Log;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use omnipaxos::messages::*;
use omnipaxos::util::NodeId;
use std::collections::HashMap;
use std::collections::HashSet;
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
            // check if the receiver is in the nodeId set
            if self.node.lock().unwrap().neighbors.contains(&receiver) {
                let channel = self
                    .outgoing
                    .get_mut(&receiver)
                    .expect("No channel for receiver");
                let _ = channel.send(msg).await;
            }
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
                    if self.node.lock().unwrap().neighbors.contains(&in_msg.get_sender()) {
                        self.node.lock().unwrap().omni_paxos_durability.omni_paxos.handle_incoming(in_msg);
                    }
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
    pub neighbors: HashSet<NodeId>,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        let omni_paxos_durability = omni_durability;
        let data_store = ExampleDatastore::new();
        let neighbors = HashSet::new();
        Node {
            node_id,
            omni_paxos_durability,
            data_store,
            neighbors,
        }
    }

    pub fn add_neighbor(&mut self, neighbor: NodeId) {
        self.neighbors.insert(neighbor);
    }

    pub fn remove_neighbor(&mut self, neighbor: NodeId) {
        self.neighbors.remove(&neighbor);
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        let curr_leader = self.omni_paxos_durability.omni_paxos.get_current_leader();
        if curr_leader == Some(self.node_id) {
            let durable_tx_offset = self.omni_paxos_durability.get_durable_tx_offset();
            let last_replicated_txns = self.data_store.get_replicated_offset();

            if last_replicated_txns < Some(durable_tx_offset) {
                let iter: Box<dyn Iterator<Item = (TxOffset, tx_data::TxData)>>;

                // check if replicated_txns is None then we need to start from the beginning of the log
                if last_replicated_txns.is_none() {
                    iter = self.omni_paxos_durability.iter();
                } else {
                    iter = self
                        .omni_paxos_durability
                        .iter_starting_from_offset(TxOffset(last_replicated_txns.unwrap().0 + 1));
                }

                for entry in iter {
                    println!("Replaying transaction for new leader: {:?}", self.node_id);
                    self.data_store
                        .replay_transaction(&entry.1)
                        .expect("Failed to replay transaction");
                }

                self.advance_replicated_durability_offset()
                    .expect("Failed to advance durable offset");
            }
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
        let last_replicated_txns = self.data_store.get_replicated_offset();
        let leader = self.omni_paxos_durability.omni_paxos.get_current_leader();

        // only the followers are allowed to replay transactions
        // check that the last replicated txns is less than the durable offset
        if last_replicated_txns < Some(durable_tx_offset) && leader != Some(self.node_id) {
            let iter: Box<dyn Iterator<Item = (TxOffset, tx_data::TxData)>>;

            // check if replicated_txns is None then we need to start from the beginning of the log
            if last_replicated_txns.is_none() {
                iter = self.omni_paxos_durability.iter();
            } else {
                iter = self
                    .omni_paxos_durability
                    .iter_starting_from_offset(TxOffset(last_replicated_txns.unwrap().0 + 1));
            }

            for entry in iter {
                println!("Replaying transaction for server: {:?}", self.node_id);
                self.data_store
                    .replay_transaction(&entry.1)
                    .expect("Failed to replay transaction");
            }
        }

        // advance the replicated offset for both the leader and the followers
        self.advance_replicated_durability_offset()
            .expect("Failed to advance durable offset");
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
            .advance_replicated_durability_offset(TxOffset(durable_tx_offset.0 - 1))
    }
}

/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really chosen (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture.
#[cfg(test)]
mod tests {
    use crate::durability::omnipaxos_durability::Log;
    use crate::durability::omnipaxos_durability::OmniPaxosDurability;
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::{LogEntry, NodeId};
    use omnipaxos::{ClusterConfig, OmniPaxosConfig, ServerConfig};
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    // for test 1 -> 3 and 4.3
    const SERVERS: [NodeId; 3] = [1, 2, 3];

    // for test 4.1, 4.2
    // const SERVERS: [NodeId; 5] = [1, 2, 3, 4, 5];

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
            let node: Arc<Mutex<Node>> = Arc::new(Mutex::new(Node::new(
                server_id,
                OmniPaxosDurability {
                    omni_paxos: op_config.build(MemoryStorage::default()).unwrap(),
                },
            )));
            for neighbor in SERVERS.iter().filter(|&&x| x != server_id) {
                node.lock().unwrap().add_neighbor(*neighbor);
            }
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

        op_server_handle
    }

    fn print_replicated_offset(nodes: &HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)>) {
        for server in nodes.values() {
            let replicated_tx = server.0.lock().unwrap().data_store.get_replicated_offset();
            println!(
                "Server {:?}, Replicated offset: {:?}",
                server.0.lock().unwrap().node_id,
                replicated_tx
            );
        }
    }
    fn print_decided_log(nodes: &HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)>) {
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
                    println!(
                        "Server: {:?}, Decided log: {:?}",
                        server.0.lock().unwrap().node_id,
                        log
                    );
                }
                // ignore uncommitted entries
            }
        }
    }

    fn print_replicated_txs(
        nodes: &HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)>,
        key: &str,
    ) {
        for server in nodes.values() {
            let tx = &server
                .0
                .lock()
                .unwrap()
                .data_store
                .begin_tx(DurabilityLevel::Replicated);
            let value = tx.get(&key.to_string());
            println!(
                "Server: {:?}, Data store(Replicated level): {:?}",
                server.0.lock().unwrap().node_id,
                value
            );
        }
    }
    /// Find the leader and commit a transaction. Show that the transaction is really chosen (according to our definition in Paxos) among the nodes.
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
        print_replicated_offset(&nodes);

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes
        for server in nodes.values() {
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        print_replicated_txs(&nodes, "foo");

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);

        // check that the committed transactions are the same for all nodes
        print_decided_log(&nodes);
    }

    /// Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
    #[test]
    fn test_2() {
        let mut runtime = create_runtime();
        let mut nodes = spawn_nodes(&mut runtime);
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

        let (leader_server, leader_join_handle) = nodes.get(&leader).unwrap();
        let (follower_server, _) = nodes.get(&follower).unwrap();
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

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes
        for server in nodes.values() {
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        print_replicated_txs(&nodes, "foo");

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);

        // check that the committed transactions are the same for all nodes
        print_decided_log(&nodes);

        // begin another mutable transaction
        let mut tx2 = leader_server
            .lock()
            .unwrap()
            .begin_mut_tx()
            .expect("Failed to begin mutable transaction");

        tx2.set("sec".to_string(), "twice".to_string());

        println!(
            "Committing mutable transaction: {:?}",
            tx2.get(&"sec".to_string())
        );
        let result2 = leader_server.lock().unwrap().commit_mut_tx(tx2).unwrap();

        // append a transaction to the OmniPaxos log
        leader_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .append_tx(result2.tx_offset, result2.tx_data);

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);
        //**************************************KILL THE LEADER************************************************************************** */
        println!("Killing leader: {}...", leader);
        leader_join_handle.abort();
        // wait for new leader to be elected...
        std::thread::sleep(WAIT_LEADER_TIMEOUT);

        let leader = follower_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .omni_paxos
            .get_current_leader()
            .expect("No leader elected");
        println!("New leader elected: {}", leader);

        // delete leader from nodes
        nodes.remove(&3);

        // update the leader and follower servers after the leader has been killed
        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes

        for server in nodes.values() {
            println!(
                "Check leader status for server: {:?}",
                server.0.lock().unwrap().node_id
            );
            server.0.lock().unwrap().update_leader();
        }

        for server in nodes.values() {
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        print_replicated_txs(&nodes, "sec");

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);

        // check that the committed transactions are the same for all nodes
        print_decided_log(&nodes);
    }

    /// Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
    /// Verify that the transaction was first committed in memory but later rolled back.
    #[test]
    fn test_3() {
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

        let (leader_server, _) = nodes.get(&leader).unwrap();
        let (follower_server, _) = nodes.get(&follower).unwrap();
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

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes
        for server in nodes.values() {
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        print_replicated_txs(&nodes, "foo");

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);

        // check that the committed transactions are the same for all nodes
        print_decided_log(&nodes);

        //**************************************DISCONNECT THE LEADER************************************************************************** */
        println!("Disconnecting leader: {}...", leader);

        // remove all neighbors from the leader server's list of neighbors
        for server_id in SERVERS.iter().filter(|&&x| x != leader) {
            leader_server.lock().unwrap().remove_neighbor(*server_id);
        }

        // remove the leader server from all other nodes list of neighbors
        for node in nodes.values() {
            let server_id = node.0.lock().unwrap().node_id;
            if server_id != leader {
                node.0.lock().unwrap().remove_neighbor(leader);
            }
        }

        // begin another mutable transaction
        let mut tx2 = leader_server
            .lock()
            .unwrap()
            .begin_mut_tx()
            .expect("Failed to begin mutable transaction");

        tx2.set("sec".to_string(), "twice".to_string());

        println!(
            "Committing mutable transaction: {:?}",
            tx2.get(&"sec".to_string())
        );
        leader_server.lock().unwrap().commit_mut_tx(tx2).unwrap();

        for server in nodes.values() {
            let tx = &server
                .0
                .lock()
                .unwrap()
                .data_store
                .begin_tx(DurabilityLevel::Memory);
            let value = tx.get(&"sec".to_string());
            println!(
                "Server: {:?}, Data store(Memory level): {:?}",
                server.0.lock().unwrap().node_id,
                value
            );
        }

        // wait for new leader to be elected...
        std::thread::sleep(WAIT_LEADER_TIMEOUT);

        let leader = follower_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .omni_paxos
            .get_current_leader()
            .expect("No leader elected");
        println!("New leader elected: {}", leader);

        // update the leader and follower servers after the leader has been disconnected
        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes

        for server in nodes.values() {
            println!(
                "Check leader status for server: {:?}",
                server.0.lock().unwrap().node_id
            );
            server.0.lock().unwrap().update_leader();
        }

        for server in nodes.values() {
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        for server in nodes.values() {
            let tx = &server
                .0
                .lock()
                .unwrap()
                .data_store
                .begin_tx(DurabilityLevel::Memory);
            let value = tx.get(&"sec".to_string());
            println!(
                "Server: {:?}, Data store(Memory level): {:?}",
                server.0.lock().unwrap().node_id,
                value
            );
        }

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);

        // check that the committed transactions are the same for all nodes
        print_decided_log(&nodes);
    }

    /// Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture.
    /// 4.1. Disconnect all nodes from each other except for the first. Verify that the leader changes.
    #[test]
    fn test_4_1() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        assert_eq!(nodes.len(), 5);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (first_server, _) = nodes.get(&1).unwrap();
        //Here we set C as leader and A as follower in the pic of Quorum-loss scenario
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

        let (follower_server, _) = nodes.get(&follower).unwrap();

        for server in nodes.values() {
            let server_id = server.0.lock().unwrap().node_id;
            //if it matches the A we skip
            if server_id == *follower {
                println!("we skip this server because it is id: {}", server_id);
            } else {
                // disconnect every node from other nodes except for the first
                for reciever in nodes.values() {
                    let reciever_id = reciever.0.lock().unwrap().node_id;
                    if reciever_id != *follower {
                        server.0.lock().unwrap().remove_neighbor(reciever_id);
                    }
                }
            }
        }

        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let new_leader = follower_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .omni_paxos
            .get_current_leader()
            .expect("No leader elected");

        // Check if the leader has changed to A
        if new_leader == *follower {
            println!("Leader has changed to QC {}", new_leader);
        } else {
            println!("The newLeader is {}", new_leader);
            panic!("Leader has not changed to QC");
        }
    }

    /// 4.2. Disconnect all nodes from each other except for the first and the last. Verify that the leader changes.
    /// outdated log scenario
    #[test]
    fn test_4_2() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        assert_eq!(nodes.len(), 5);
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

        let (leader_server, _) = nodes.get(&leader).unwrap();
        let (follower_server, _) = nodes.get(&follower).unwrap();

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

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes
        for server in nodes.values() {
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        print_replicated_txs(&nodes, "foo");

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);

        // begin another mutable transaction
        let mut tx2 = leader_server
            .lock()
            .unwrap()
            .begin_mut_tx()
            .expect("Failed to begin mutable transaction");

        tx2.set("sec".to_string(), "twice".to_string());

        println!(
            "Committing mutable transaction: {:?}",
            tx2.get(&"sec".to_string())
        );
        let result2 = leader_server.lock().unwrap().commit_mut_tx(tx2).unwrap();

        // append a transaction to the OmniPaxos log
        leader_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .append_tx(result2.tx_offset, result2.tx_data);

        std::thread::sleep(WAIT_DECIDED_TIMEOUT);
        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes except for the follower
        // since we want to have outdated logs
        for server in nodes.values() {
            if server.0.lock().unwrap().node_id != *follower {
                server.0.lock().unwrap().apply_replicated_txns();
            }
        }
        std::thread::sleep(WAIT_DECIDED_TIMEOUT);

        //*************************        // check the data stores for all nodes
        print_replicated_txs(&nodes, "sec");

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);

        // *************Delete THE EDGES************************************************************************** */
        // for A (node 1) we keep all the connections
        println!("Deleting edges...");
        for server in nodes.values() {
            let server_id = server.0.lock().unwrap().node_id;
            //if it matches A (node 1) we skip
            if server_id != *follower {
                // disconnect every node from other nodes except for the first
                for reciever in nodes.values() {
                    let reciever_id = reciever.0.lock().unwrap().node_id;
                    if reciever_id != *follower {
                        server.0.lock().unwrap().remove_neighbor(reciever_id);
                    }
                }
            }
        }
        //remove egdes bewteen the leader and the follower(A,C)
        follower_server.lock().unwrap().remove_neighbor(leader);
        leader_server.lock().unwrap().remove_neighbor(*follower);
        //delete leader from nodes
        // wait for new leader to be elected...
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let leader = follower_server
            .lock()
            .unwrap()
            .omni_paxos_durability
            .omni_paxos
            .get_current_leader()
            .expect("No leader elected");
        println!("New leader elected: {}", leader);

        // update the leader and follower servers after the leader has been killed
        // apply the committed transactions to the follower servers and advance the replicated offset to all nodes

        for server in nodes.values() {
            println!(
                "Check leader status for server: {:?}",
                server.0.lock().unwrap().node_id
            );
            server.0.lock().unwrap().update_leader();
        }

        for server in nodes.values() {
            server.0.lock().unwrap().apply_replicated_txns();
        }

        // check the data stores for all nodes
        print_replicated_txs(&nodes, "sec");

        // check that replicated offset is the same for all nodes
        print_replicated_offset(&nodes);
    }

    /// 4.3. Chained scenario.
    #[test]
    fn test_4_3() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        assert_eq!(nodes.len(), 3);
        std::thread::sleep(WAIT_LEADER_TIMEOUT);
        let (first_server, _) = nodes.get(&1).unwrap();
        //Here we set C as leader and A as follower in the pic of Chained scenario
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

        // remove connection between node 2 and 3
        println!("Removing connection between node 2 and 3...");

        for server in nodes.values() {
            let server_id = server.0.lock().unwrap().node_id;
            //if it matches node 1 we skip
            if server_id != *follower {
                for reciever in nodes.values() {
                    let reciever_id = reciever.0.lock().unwrap().node_id;
                    if reciever_id != *follower && reciever_id != server_id {
                        println!(
                            "Server: {:?}, Removing neighbor: {:?}",
                            server.0.lock().unwrap().node_id,
                            reciever_id
                        );
                        server.0.lock().unwrap().remove_neighbor(reciever_id);
                    }
                }
            }
        }

        // wait for new leader to be elected...
        for timer in 0..10 {
            std::thread::sleep(WAIT_LEADER_TIMEOUT);
            println!("Check if there is livelock, time: {}...", timer);
            for server in nodes.values() {
                let leader = server
                    .0
                    .lock()
                    .unwrap()
                    .omni_paxos_durability
                    .omni_paxos
                    .get_current_leader()
                    .expect("No leader elected");
                println!(
                    "Server {:?} following leader {:?}",
                    server.0.lock().unwrap().node_id,
                    leader
                );
            }
        }
    }
}
