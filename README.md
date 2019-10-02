# Simple Amazon Dynamo
A simplified implementation of the distributed Key-Value storage [Amazon Dynamo](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf) on Android Studio. 

## Basic Working
The app provides linearizability as well as availability in case of failure of nodes. For the sake of our implementation, we use 5 Android Studio AVDs connected to each other over network. The nodes are able to communicate with each other through messages sent via Socket connections. The messages sent are stored as files such that the filename functions as the key and the content of the file functions as the value. 

## Amazon Dynamo Implementation

<b>1. Membership:</b> <br>
Each node(AVD) is aware of the other nodes present in the network. A node is uniquely identified using the AVD emulator ID. We use the emulator IDs to compute the hash value of the node. The hash space can be thought of as a virtual ring-like structure where each node has its own partition of the hash space. In accordance with the workings of the Amazon Dynamo, each node is aware of the presence/absence of the nodes in the ring structure.

<b>2. Request Routing:</b> <br> 
Each node can correctly determine the correct node partition in the hash space to which the incoming request belongs to and forward the request accordingly.

<b>3. Quorum Replication:</b> <br> 
With every successful insert completed at a given node, the node replicates the given key-value at its two subsequent node(successor nodes) in the hash ring.

<b>4. Failure Handling:</b> <br>
Incase of node failure, the node with the incoming request holds on to the message until the failed node comes back. Meanwhile, the node attempts to replicate the message at the correct node's successors. In case the successor nodes have failed as well, the node holds on to the failed messages for the successor nodes as well.

