# Partitioned Transactional Outbox Design Document

## 1. Overview

### Current State
The existing transactional outbox implementation uses ShedLock to ensure only one node processes outbox events at a time. This approach has limitations:
- Single point of processing bottleneck
- Underutilized resources in multi-node deployments
- Processing capacity doesn't scale with node count

### Proposed Solution
Implement a partitioned transactional outbox pattern that allows multiple nodes to process events concurrently by:
- Partitioning outbox events logically
- Distributing partitions across available nodes
- Using per-partition locking instead of global locking

## 2. Architecture Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Node 1    │    │   Node 2    │    │   Node 3    │
│ Partitions  │    │ Partitions  │    │ Partitions  │
│   0, 3      │    │   1, 4      │    │   2, 5      │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
              ┌─────────────────────────┐
              │     Database            │
              │  ┌─────────────────┐    │
              │  │ outbox_events   │    │
              │  │ - partition_id  │    │
              │  └─────────────────┘    │
              │  ┌─────────────────┐    │
              │  │ outbox_nodes    │    │
              │  │ - node_id       │    │
              │  │ - last_heartbeat│    │
              │  └─────────────────┘    │
              └─────────────────────────┘
```

## 3. Core Components

### 3.1 Partition Assignment Service
**Responsibility**: Determines which partitions each node should process

**Key Features**:
- Consistent hashing for even distribution
- Automatic rebalancing when nodes join/leave
- Configurable total partition count

**Algorithm**:
```
For N active nodes and P total partitions:
Node i processes partitions: {p | p % N == i}

Example: 6 partitions, 3 nodes
- Node 0: partitions [0, 3]
- Node 1: partitions [1, 4] 
- Node 2: partitions [2, 5]
```

### 3.2 Node Registry Service
**Responsibility**: Track active nodes via heartbeat mechanism

**Key Features**:
- Periodic heartbeat updates (30s interval)
- Automatic cleanup of expired nodes (2 minute timeout)
- Database-backed persistence for cluster coordination

### 3.3 Partitioned Outbox Processor
**Responsibility**: Process outbox events for assigned partitions

**Key Features**:
- Per-partition ShedLock instead of global lock
- Parallel processing of assigned partitions
- Maintains existing Kafka publishing logic

## 4. Database Schema Changes

### 4.1 Reuse Existing ShedLock Table
No new tables needed! We'll use the existing ShedLock table for node registration:

```sql
-- ShedLock table (already exists)
-- We'll use lock_name pattern: "node-heartbeat-{nodeId}"
-- The locked_at timestamp serves as our heartbeat
-- The locked_until tells us when the node expires

-- Example entries:
-- lock_name: "node-heartbeat-pod-abc123"  
-- locked_at: 2024-01-15 10:30:00
-- locked_until: 2024-01-15 10:32:00  (heartbeat + 2 minutes)
```

### 4.2 Modified Table: outbox_events
```sql
-- Add partition_id column if not exists
ALTER TABLE outbox_events ADD COLUMN partition_id INT NOT NULL DEFAULT 0;

-- Add index for efficient partition-based queries
CREATE INDEX idx_outbox_events_partition_status ON outbox_events(partition_id, status, created_at);
```

## 5. Detailed Component Design

### 5.1 PartitionAssignmentService

```java
@Service
public class PartitionAssignmentService {
    private final String nodeId;
    private final int totalPartitions;
    
    public Set<Integer> getAssignedPartitions(Set<String> activeNodes);
    public int calculatePartition(String key);
}
```

**Configuration**:
- `outbox.node.id`: Unique node identifier (defaults to hostname/UUID)
- `outbox.total.partitions`: Total number of partitions (default: 6)

### 5.2 NodeRegistryService (ShedLock-based)

```java
@Service 
public class NodeRegistryService {
    private static final String NODE_HEARTBEAT_PREFIX = "node-heartbeat-";
    private static final Duration NODE_TIMEOUT = Duration.ofMinutes(2);
    
    private final ShedLockTemplate shedLockTemplate;
    private final LockProvider lockProvider;
    
    public void updateNodeHeartbeat(String nodeId) {
        // Use ShedLock to register/update node heartbeat
        String lockName = NODE_HEARTBEAT_PREFIX + nodeId;
        
        LockConfiguration lockConfig = new LockConfiguration(
            Instant.now(),
            lockName, 
            NODE_TIMEOUT,        // locked_until = now + 2 minutes
            Duration.ofSeconds(1) // locked_at_least_for
        );
        
        shedLockTemplate.executeWithLock(() -> {
            // This execution updates the heartbeat automatically
            log.debug("Heartbeat updated for node: {}", nodeId);
        }, lockConfig);
    }
    
    public Set<String> getActiveNodes() {
        // Query ShedLock table for active node heartbeats
        return lockProvider.findActiveNodeLocks(NODE_HEARTBEAT_PREFIX, Instant.now());
    }
}
```

**Benefits of ShedLock approach**:
- **No table bloat**: Expired locks are automatically cleaned up
- **Reuse existing infrastructure**: No new tables or maintenance
- **Atomic operations**: Lock acquisition = heartbeat update
- **Built-in expiration**: `locked_until` serves as node timeout

### 5.3 TransactionOutboxProcessor (Modified)

```java
public class TransactionOutbox {
    @Scheduled(fixedRate = 10000)
    public void publishToKafka();
    
    @Scheduled(fixedRate = 30000) 
    @SchedulerLock(name = "NodeHeartbeat")
    public void updateNodeHeartbeat();
    
    private void processPartition(Integer partitionId);
}
```

**Lock Strategy**:
- Global heartbeat lock: `NodeHeartbeat`
- Per-partition locks: `outbox.task.partition.{partitionId}`

## 6. Operational Scenarios

### 6.1 Normal Operation (6 partitions, 3 nodes)
```
Node A: processes partitions [0, 3]
Node B: processes partitions [1, 4]  
Node C: processes partitions [2, 5]
```

### 6.2 Node Failure (Node B fails)
```
Before: A[0,3], B[1,4], C[2,5]
After:  A[0,2,4], C[1,3,5]

Automatic rebalancing within 2 minutes
```

### 6.3 Node Addition (4th node joins)
```
Before: A[0,3], B[1,4], C[2,5] (3 nodes)
After:  A[0], B[1], C[2], D[3,4,5] (4 nodes)

Gradual rebalancing as nodes discover each other
```

### 6.4 Scaling Scenarios
| Partitions | Nodes | Distribution | Idle Nodes |
|------------|-------|--------------|------------|
| 6          | 6     | 1:1          | 0          |
| 6          | 3     | 2:1          | 0          |
| 6          | 8     | ~1:1         | 2          |

## 7. Configuration

### 7.1 Application Properties
```yaml
outbox:
  node:
    id: ${HOSTNAME:#{T(java.util.UUID).randomUUID().toString()}}
  total:
    partitions: 6
  task:
    scheduler:
      publish:
        fixedRate: 10000        # 10 seconds
        initialDelay: 10000
      heartbeat:
        fixedRate: 30000        # 30 seconds
        lockAtLeastFor: PT29S

shedlock:
  defaults:
    lock-at-most-for: PT5M
    lock-at-least-for: PT30S
```

### 7.2 Environment Variables
- `HOSTNAME`: Used as default node identifier
- `OUTBOX_PARTITIONS`: Override total partition count
- `OUTBOX_NODE_ID`: Override node identifier

## 8. Migration Strategy

### 8.1 Phase 1: Database Schema
1. Add `outbox_nodes` table
2. Add `partition_id` column to `outbox_events`
3. Populate `partition_id` for existing events
4. Add required indexes

### 8.2 Phase 2: Code Deployment
1. Deploy new code with feature flag disabled
2. Verify backward compatibility
3. Enable partitioned processing
4. Monitor partition distribution

### 8.3 Phase 3: Validation
1. Verify even partition distribution
2. Test node failure scenarios
3. Monitor processing latency and throughput
4. Remove old global lock logic

## 9. Monitoring and Observability

### 9.1 Key Metrics
- **Partition Distribution**: Events per partition per node
- **Node Health**: Active nodes count, heartbeat status
- **Processing Latency**: Time from event creation to publication
- **Lock Contention**: Failed lock acquisition attempts
- **Rebalancing Events**: Frequency of partition reassignment

### 9.2 Logging Strategy
```java
// Structured logging examples
log.info("Node {} assigned partitions: {}", nodeId, partitions);
log.debug("Processing {} events from partition {}", count, partitionId);
log.warn("Node {} not responding, rebalancing partitions", failedNodeId);
```

### 9.3 Health Checks
- Node registry connectivity
- Partition assignment validation
- Database connectivity per node
- Kafka producer health

## 10. Error Handling and Resilience

### 10.1 Database Failures
- **Node Registry Unavailable**: Fall back to single-node processing
- **Connection Timeouts**: Retry with exponential backoff
- **Lock Acquisition Failures**: Skip partition processing cycle

### 10.2 Kafka Failures
- **Producer Errors**: Mark events as failed, retry later
- **Broker Unavailable**: Circuit breaker pattern
- **Serialization Errors**: Dead letter queue for problematic events

### 10.3 Node Management
- **Split Brain Prevention**: Use database as single source of truth
- **Graceful Shutdown**: Release locks before termination
- **Startup Coordination**: Wait for stable node registry before processing

## 11. Performance Considerations

### 11.1 Throughput Improvements
- **Parallel Processing**: N nodes = N× throughput potential
- **Reduced Lock Contention**: Per-partition locks vs global lock
- **Better Resource Utilization**: All nodes actively processing

### 11.2 Latency Considerations
- **Partition Assignment Delay**: Max 30 seconds for rebalancing
- **Lock Acquisition**: 30 second timeout per partition
- **Node Discovery**: 2 minute timeout for failed node detection

### 11.3 Resource Usage
- **Database Connections**: Monitor connection pool usage
- **Memory**: Node registry cached in memory
- **CPU**: Parallel partition processing increases CPU usage

## 12. Testing Strategy

### 12.1 Unit Tests
- Partition assignment algorithm correctness
- Node registry lifecycle management
- Lock acquisition and release logic

### 12.2 Integration Tests
- Multi-node scenario simulation
- Database schema migration validation
- Kafka integration with partitioned events

### 12.3 Load Tests
- Throughput comparison: single vs multi-node
- Partition hotspot identification
- Resource utilization under load

### 12.4 Chaos Testing
- Random node failures
- Database connectivity issues
- Network partitions between nodes

## 13. Security Considerations

### 13.1 Node Authentication
- Verify node identity before registry updates
- Prevent malicious nodes from joining cluster
- Audit trail for node registry changes

### 13.2 Database Security
- Encrypt sensitive data in outbox events
- Row-level security for partition access
- Connection pooling security

## 14. Future Enhancements

### 14.1 Dynamic Partitioning
- Auto-scale partition count based on load
- Partition splitting/merging strategies
- Hot partition detection and mitigation

### 14.2 Advanced Load Balancing
- Weighted partition assignment based on node capacity
- Priority-based event processing
- Geographic partition distribution

### 14.3 Operational Improvements
- Web dashboard for cluster monitoring
- Automated partition rebalancing triggers
- Integration with service discovery systems

## 15. Rollback Plan

### 15.1 Code Rollback
1. Disable partitioned processing via feature flag
2. Revert to single global lock processing
3. Monitor for processing continuity
4. Deploy previous version if needed

### 15.2 Schema Rollback
1. Remove partition-based indexes
2. Drop `partition_id` column (data loss)
3. Drop `outbox_nodes` table
4. Restore original schema

### 15.3 Data Recovery
- Backup strategy before migration
- Point-in-time recovery procedures
- Event replay mechanisms

## 16. Success Criteria

### 16.1 Performance Metrics
- **Throughput**: 3x improvement with 3 nodes
- **Latency**: <10% increase in P95 processing time
- **Availability**: >99.9% uptime during node failures

### 16.2 Operational Metrics
- **Zero Data Loss**: All events processed exactly once
- **Fast Recovery**: <2 minutes for node failure detection
- **Smooth Scaling**: Linear throughput scaling with node count

---

**Document Version**: 1.0  
**Last Updated**: [Current Date]  
**Authors**: [Team Name]  
**Reviewers**: [Architecture Team]