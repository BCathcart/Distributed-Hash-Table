# miniproject
The CPEN 431 Mini-Project - DHT using Consistent Hashing Test Edit

# miniproject
The CPEN 431 Mini-Project - DHT using Consistent Hashing Test Edit

### Team Members
Name | github user
------------ | -------------
Brennan Cathcart | BCathcart
Shay Tanne | rozsa
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
- Failure detection could be done completely locally (like Cassandra)
    - Each node comes to their own conclusion that a node has failed if they cannot route a request to it and sets its status as "Unavailable" locally 
    - If they are wrong about the node failing, they would simply forward requests to the wrong node which would then forward the requests to the right node
    - When the successor node detects that the node has failed, it will take ownership of the failed node’s keys (it will handle any requests it receives with keys the suspected failed node is responsible for)
    - If the successor then receives a heartbeat from the suspected failed node, it will transfer any new data it stored for the keys it temporarily took ownership of (not yet implemented)



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

Periodically check the request queue for timed out messages. If an internal message timed out once, resend it. If a response still isn't received, send a ping to check if it's available.

Types of Messages:
- External messages handled by App layer
- Internal messages handled by Membership Service

Membership Store
```
type ReqCacheEntry struct {
    msg        []byte
    time       time.Time
    retries    uint8
    returnAddr *net.Addr
    firstHop   bool // Used so we know to remove "internalID" from response
    clientReq  bool // Used to make sure we don't retry a client request ourselves
}
```
