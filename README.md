# Raft - Replicated State Machine Protocol Implementation
This project involved implementing Raft, a consensus algorithm for managing a replicated log in a fault-tolerant distributed system. The main objective was to achieve high availability and consistency by replicating data across multiple servers, ensuring that even in the event of server or network failures, the system remains operational as long as a majority of nodes are reachable.

Raft achieves this by:

## Leader Election: 
Servers coordinate to elect a leader, which becomes responsible for handling client requests and managing log replication. This component required handling election timeouts, sending and receiving vote requests (via RequestVote RPCs), and ensuring that a single leader is elected in each term. Leaders also maintain their role by periodically sending heartbeats (empty AppendEntries RPCs) to followers, resetting their election timers.

## Log Replication: 
The elected leader appends client requests to its local log and replicates these log entries to followers through AppendEntries RPCs. By maintaining identical logs across nodes and committing entries only when they are replicated on a majority of servers, Raft ensures that all nodes maintain a consistent state.

## Persistence: 
To ensure resilience in the face of server crashes, the Raft implementation includes state persistence. Persistent data, such as current term and log entries, are stored in a manner that allows servers to recover and resume from their previous state upon restart.

## Concurrency and Failure Handling: 
This project involved extensive use of Go’s concurrency model to manage timed events, such as election timeouts, leader heartbeats, and background RPC handlers. The system is designed to handle issues like network partitions, random failures, and message reordering or loss.

The implementation adheres closely to Raft’s design as specified in its foundational paper, focusing on modularity, fault tolerance, and data consistency across nodes. This approach ensures that each replica holds a consistent view of the log, applying commands in the same order to maintain identical states.
