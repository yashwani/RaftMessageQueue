# Raft Message Queue #


![Raft Mascot](/img/raft.png)


This project's aim is to create a strongly consistent, fault tolerant message 
queue distributed across multiple nodes. The RAFT consensus algorithm is used to 
ensure fault tolerance and consistency despite failures in a minority of nodes. 

In this python 3+ implementation, the ZeroMQ asynchronous messaging library is used 
for all communication. Each node is assigned an internal port for swarm 
communication and an external port for client communication when in the "Leader" 
role. In this
particular implementation, only leaders have the ability to respond to clients,
and message forwarding is not enabled for followers. 

Upon initialization of raftnode, three threaded event loops are created. One 
sets up a receiver for external events at the external port. This 
receiver’s message handler is a blocking function that waits for 
all internal node communication to complete before returning back to
the client. The second is a receiver for internal communication at 
the internal port. The internal receiver immediately sends back a dummy reply.
This means that all communication, whether a request or response, is
initiated through the ZMQ REQ socket. All ZMQ REP socket internal communications 
are “dummy” replies. The internal message handler then proceeds based 
on the state of the node and the contents of the message. Finally, given
the state of the node, an election timer or heartbeat timer is created.

Sample system with 3 nodes:
```azure
> python3 src/node.py config.json 0 
> python3 src/node.py config.json 1
> python3 src/node.py config.json 2 
```
