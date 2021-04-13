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
- We updated our transfer system in the chain replication layer to handle any number of node failures at the same time. For M2, our system struggled when more than one node failed at a time.
- To keep our system easy to reason about when multiple changes happen at the same time, we created a queue for replication events and we handle them sequentially. Replication events are either transfer requests or sweeps (drop unneeded keys).
  - When the handling of the previous event is finished, the next one can proceed
- When the keyspace the node is responsible for (coordination + replication range) expands, a transfer event is added
- When the keyspace the node is responsible for (coordination + replication range) shrinks because more nodes joined, a sweep event is added
- **Sweep Event**: removes all keys in the store outside its responsible range
- **Transfer Event**: requests a transfer for the keys it is missing from the nodes predecessor
  - Partial transfers can take place within one transfer event when the predecessor only has some of the keys. Each time a partial transfer is received, the key range being requested is updated accordingly. The requesting node continues waiting for some time expecting the predecessor to obtain the missing keys from its own predecessor.
- All transfer scenarios when a single node fails and joins are detailed here along with their corresponding actions: https://app.diagrams.net/#G1MaVQbmbZ6cjkAzkG8zbFdaj9r03HWV5A

## Chain Replication - routing
- Updates (PUT, REMOVE, WIPEOUT) are routed to the head of the chain (i.e. the coordinator of the key) which performs the update and then forwards it up the chain
- GET requests are routed to the tail. The tail node then responds to the client directly, and sends the response back down the chain, which gets 
- forwarded back to the tail.
- All other client requests (that are not key-value requests) are handled at the receiving node.

## Sequential Consistency
- In order to achieve sequential consistency, updates and GET requests that are routed to each node are queued and processed in order of receipt. However, the current design does not guarrantee that update requests reach the successor in the same order and arbitrary communication delays could cause violations to sequential consistency. 
- Note: To remedy this, a modified protocol was implemented in m2-responsehandling in which the head node waits for a response from the tail (which is routed through the seond node in the chain) that it has received the update before proceeding with the next update. If any node in the chain fails to process the request or the head does not receive a response within a specified timeout period, the head node reverses the update operation and moves on to the next request. The second node in the chain behaves similarly if it does not receive a response from the tail. Due to insufficient testing, this modification was not included in the milestone2 submission. 

## Integration Testing
### Overview
- To help test our code, we extended the client from the individual programming assignments.
- To run the tests, open testDriver.go in the integrationTest, edit the parameters, then run main
- One of the main features of these tests is being able to send keys that will always be handled by certain nodes.
  - This is done by getting the keyRange of each node in the system, then sending keys that will be hashed
  to always be handled by a desired target node.
- There are three tests that can be run
  - PrintKeyRange prints out the order of nodes in the chain based on port value.
    - This is useful for testing the replication, e.g. could send keys to land on nodes
    close to each other in the hashing ring
    - It can also be used to shut down two nodes next to each other to test recovery
  - PutGetTest sends a set amount of keys, then attempts to retrieve them
  - shutDownTest is an extension of the PutGetTest. It sends keys, shuts down nodes and
    then tries to retrieve the keys.
    - Often it is useful to send keys that will only land on the nodes which will be killed
### Potential future work
- Aside from adding more tests, two areas for improvement would be adding a retry mechanism and
removing the sleep function
  - The retry function could be done similarly to the sweepReqCache() function in our own client
  - Currently, because the messages are received in a separate goRoutine, it was challenging to figure
  out when to stop the tests and analyze the results. As a simple fix, a long timeout (roughly 15s) was put 
    at the end of tests in order to always have sufficient time to receive them. This could open errors in the future
    and make the test slower than necessary.
- Another small improvement could be to use a .json file for the arguments rather than editing the go code directly.
Using command line arguments was considered but did not seem practical since there were so many arguments.
    

