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

### Design Overview

Our distributed key-value store is organized in 3 major layers of functionality, each contained within its own package:
* `kvStore` - Handles access to the key-value store functionality inside a server node
* `membership` - Provides the membership service using q gossip-style protocol
* `requestreply` - Handles internal (server-server) and external (client-server) communications

There is an additional package `utils`, which includes helper functions, utilities, support for hashing etc. 

Our implementation 
* Availability - client requests are forwarded 
* Failure detection - 
* Response cache - 
