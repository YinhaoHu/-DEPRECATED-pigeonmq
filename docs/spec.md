# RPC Specification
**AppendPrimarySegment**

Bookie Ensemble allows the duplicate append to occur but does not allow the wrong logs to occur if
MinimumReplicaNumber is more than half of ReplicateNumber. Wrong logs mean two entries in the same position
in two logs are different. If MinimumReplicaNumber <= ReplicateNumber/2, the wrong logs are allowed by
the implementation.

What will happen when leader crash before the leader response? 

Nothing except the client will receive the timeout error and will inquire the 
zk to find the new leader again. This time, the duplicate append will happen if
one of the followers has already received the append message before the leader
crash which is acceptable by our implementation.

What will happen when follower crash?

If the number of available followers are not less than MinimumReplicaNumber, 
the leader can still move on and response to the client. If that's not the case,
the RPC request will time out most probably and the client will retry.

Concurrency should be made correct by using distributed lock.

Note: MinimumReplicaNumber, ReplicateNumber are both config entries.

**CreatePrimarySegment**

What if errors happen?

`UpdateBookieStateOnZK` error does not matter at all because it has no need to 
make this exactly correct because this information is used in load balance 
and does not make influence to correctness. However, if this error happened once,
it will be fixed in the next update and too many errors like this are not possible.
