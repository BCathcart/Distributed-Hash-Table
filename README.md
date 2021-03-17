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

### Design Overview

Our distributed key-value store is organized in 3 major layers of functionality, each contained within its own package:
* `kvStore` - Handles access to the key-value store functionality inside a server node
* `membership` - Provides the membership service using q gossip-style protocol
* `requestreply` - Handles internal (server-server) and external (client-server) communications

There is an additional package `utils`, which includes helper functions, utilities, support for hashing etc. 

Our implementation 
* Availability
  * Client requests are forwarded by bootsrapping nodes to avoid blocking clients
  * Key transfer operation - transfering keys to a joining node is the heaviest operation in the system. We avoid blocking client requests during a transfer operation by sending keys individually, making the putting a new keys the only operation that is blocked
* Request/ Response Cache
  * Requests that need acknowledging are stored in the request cache
  * Responses are asynchronously received and matched to the request cache entry. 
  * The request cache automatically re-sends internal messages after 250ms without a response. External request retries are the responsibility of the client.
  * After a request has timed out, a PING message is sent to check if the node is still available.

