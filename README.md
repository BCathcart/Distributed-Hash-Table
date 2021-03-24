# miniproject
The CPEN 431 Mini-Project - DHT using Consistent Hashing Test Edit

### Team Members
Name | github user
------------ | -------------
Brennan Cathcart | BCathcart
Rozhan Akhound-Sadegh | rozsa
Shay Tanne | shaytanne
Thomas Broatch | tbroatch98

<p>&nbsp;</p>


## Design
- Every node has a heartbeat that is incremented every second (will serve a similar purpose to a logical clock)
- There are two possible node statuses:
    - “Bootstrapping” and “Normal”
        - At this time, I think we can assume no nodes ever want to leave the group (so we don't need a "Leaving" status).
    - Every node randomly chooses another node to gossip with every second and exchanges the latest membership info (includes status and heartbeat they have for each node)
        - Can determine which data is newer based on the heartbeat numbers
- Joining the cluster:
    - When a node first wants to join, it sets its status as “Bootstrapping”
    - The node makes a "membership request" that gets directed to its successor node which starts the transfer.
    - The node periodically re-sends this request until the transfer finishes to account for the successor failing.
    - Once the successor to the new node receives the membership request, it starts transferring the necessary keys
    - In the meantime, all requests for keys that the new node is responsible for still get first sent to the successor while the status is "Bootstrapping"
    - The successor can forward requests to the node it is transferring to as needed.
    - Once the transfer has finished (or the successor happens to fail so that the transfer ends), then the new node’s status is set to “Normal” which will be gossiped around
    - All other nodes will now forward requests with keys the new node is responsible for directly to the new node once they get this info through gossip


## Request/Reply Protocol
- Sending Requests
    - Send the request and cache it along with any other info needed for handling the response
    - For example, forwarded messages will need the address of the previous sender stored
    - Incoming Requests
    - If can handle at the node, do so and send a response in one goroutine
    - If need to forward request, follow "Sending Requests"
- Incoming Responses
    - If the message is queued with a return address, then forward the response to the return address
    - If the message is queued without a return address, handle the response
    - I don't believe anything needs to be handled in our current cases. Requiring responses in these cases is solely for detecting "Unavailable" nodes.
    - In both cases remove the corresponding request from the request cache
- Failures
  - Each node comes to their own conclusion that a node has failed if they cannot route a request to it and sets its status as "Unavailable" locally
  - If they are wrong about the node failing, they would simply forward requests to the wrong node which would then forward the requests to the right node
  - When the successor node detects that the node has failed, it will take ownership of the failed node’s keys (it will handle any requests it receives with keys the suspected failed node is responsible for)
Periodically check the request queue for timed out messages. If an internal message timed out once, resend it. If a response still isn't received, send a ping to check if it's available.

Types of Messages:
- External messages handled by App layer
- Internal messages handled by Membership Service

Membership Store
```
type ReqCacheEntry struct {
    msgType    uint8  // i.e. Internal ID
	msg        []byte // serialized message to re-send
	time       time.Time
	retries    uint8
	addr       *net.Addr
	returnAddr *net.Addr
	isFirstHop bool // Used so we know to remove "internalID" and "isResponse" from response
}
```

## Chain Replication - general design
- For M2, our design uses chain replication
  - To maintain a replication factor of 3, keys that belong to a node will also be replicated at the next 2 nodes.
  - To ensure that the data is being replicated correctly through the chain, the last node in the chain will respond to the client.
  - In the chainReplication package, each node will keep track of its three predecessors (the first three nodes before 
    a node in the ring) and one successor (the first node after a node in the ring). 
    - This allows the nodes to detect changes to the keyspace (failures or new nodes)

NOTE: When we heard there would be “low churn”, we incorrectly assumed this meant it was highly unlikely multiple nodes would fail at the same time. With our current design, there is a chance data is lost if two nodes in the same chain fail at the exact same time. Right now we have a push-based system, but we plan to switch to a pull-based system for M3 where each node keeps track of which keyspace it has (including replicas) and hounds their predecessor for a transfer if the keyspace they're responsible for replicating grows.

## Chain Replication - handling keyspace changes 
- When a node detects changes to its predecessors / successors, the updatePredecessor and updateSuccessor functions 
  will determine the appropriate course of action based on the node's relative position to the new / failed node.
    - In general, a failed predecessor / successor node means that our node will need to transfer some of its keys to a successor node, in order to maintain
      a replication factor of 3. A new predecessor / successor node joining means that the node is no longer responsible for some of its keys and should remove them from its store.
    - All scenarios and corresponding actions are listed here: https://app.diagrams.net/#G1MaVQbmbZ6cjkAzkG8zbFdaj9r03HWV5A
    - These scenarios above are also tested in the replicationManager_test file

## Chain Replication - routing
- Updates (PUT, REMOVE, WIPEOUT) are routed to the head of the chain (i.e. the coordinator of the key) which performs the update and then forwards it up the chain
- GET requests are routed to the tail. The tail node then responds to the client directly, and sends the response back down the chain, which gets 
- forwarded back to the tail.
- All other client requests (that are not key-value requests) are handled at the receiving node.

## Sequential Consistency
- In order to achieve sequential consistency, updates and GET requests that are routed to each node are queued and processed in order of receipt. However, the current design does not guarrantee that update requests reach the successor in the same order and arbitrary communication delays could cause violations to sequential consistency. 
- Note:
- To remedy this, a modified protocol was implemented in m2-responsehandling in which the head node waits for a response from the tail (which is routed through the seond node in the chain) that it has received the update before proceeding with the next update. If any node in the chain fails to process the request or the head does not receive a response within a specified timeout period, the head node reverses the update operation and moves on to the next request. The second node in the chain behaves similarly if it does not receive a response from the tail.
- Due to insufficient testing, this modification was not included in the milestone2 submission. 

## Integration Testing
- To help test our code, we extended the client from the individual programming assignments.
- This helped with debugging in the earlier phases of development for this milestone
  - However, due to changes in how we handle messages (the node that sends
  messages back to the client is not the same node that the client sends its requests to) they are currently not
  fully functional, but we plan of have them updated for milestone 3.
