// Improved Multi-Node Kafka Publisher - Enhanced Robustness

// Configuration properties (same as before)
@Value("${outbox.task.scheduler.publish.multinode.enabled:false}")
private boolean multiNodeEnabled;

@Value("${outbox.task.scheduler.publish.multinode.heartbeat.interval:60000}") // 1 minute default
private long heartbeatInterval;

@Value("${outbox.task.scheduler.publish.multinode.total.partitions:6}")
private int totalPartitions;

@Value("${spring.application.name:default-app}")
private String applicationName;

@Autowired
private PartitionAssignmentRepository partitionAssignmentRepository;

@Autowired
private ShedLockRepository shedLockRepository;

private final String nodeId = generateNodeId();

/**
 * Unified publishing method - handles both single and multi-node modes
 * FIXED: Per-node locking for parallel processing
 */
@Scheduled(fixedRateString = "${outbox.task.scheduler.publish.fixedRate:10000}")
@SchedulerLock(name = "${outbox.task.shedlock.publish.name:TaskScheduler_publishEventTask}_" + nodeId, lockAtLeastFor = "PT9S")
public void publishToKafka() {
        try {
        List<EventEntity> eventsRetrieved;
        String context;

        if (multiNodeEnabled) {
        // Register/update this node in ShedLock table
        registerNodeInShedLock();

        // Get assigned partitions for this node
        List<Integer> assignedPartitions = getAssignedPartitions();

        if (assignedPartitions.isEmpty()) {
        log.debug("No partitions assigned to node: {}", nodeId);
        return;
        }

        log.debug("Node {} processing partitions: {}", nodeId, assignedPartitions);
        context = "partitions " + assignedPartitions;

        // Query by partitions
        eventsRetrieved = eventRepository.findUnpublishedEventsByPartitions(assignedPartitions, batchSize);
        } else {
        context = "all events";

        // Query all unpublished events
        eventsRetrieved = eventRepository.findUnpublishedEvents(batchSize);
        }

        // Common processing logic for both modes
        processEvents(eventsRetrieved, context);

        } catch (Exception ex) {
        log.error("Error in publishing for node: {}", nodeId, ex);
        }
        }

/**
 * Common event processing logic - used by both single and multi-node modes
 */
private void processEvents(List<EventEntity> eventsRetrieved, String context) {
        if (eventsRetrieved.isEmpty()) {
        return;
        }

        ExecutorService customPool = ExecutorUtil.getCustomPool(asyncPool);
        ExecutorService kafkaProducerHandlerPool = ExecutorUtil.getKafkaProducerHandlerPool(asyncPool);

        Instant publishStart = Instant.now();
        List<CompletableFuture<Void>> futures = eventsRetrieved.stream()
        .map(eventRetrieved -> CompletableFuture.runAsync(() -> {
        log.debug("Started asyncPool number : {}", Thread.currentThread().getName());
        byte[] payload = eventRetrieved.getPayload();
        byte[] key = eventRetrieved.getKey();
        kafkaTemplate.send(eventRetrieved.getTopic(), key, payload)
        .thenAccept(sendResult -> {
        log.debug("Success");
        eventRetrieved.setPublishedTime(new Timestamp(sendResult.getRecordMetadata().timestamp()));
        eventRetrieved.setPublishedStatus(true);
        eventRetrieved.setAckTime(new Timestamp(System.currentTimeMillis()));
        eventRetrieved.setPartitionId(String.valueOf(sendResult.getRecordMetadata().partition()));
        }, kafkaProducerHandlerPool)
        .exceptionallyAsync(ex -> {
        log.error("Error when publishing event = {}", ex);
        eventRetrieved.setPublishedStatus(false);
        eventRetrieved.setRetryCount(eventRetrieved.getRetryCount() + 1);
        eventRetrieved.setAckTime(new Timestamp(System.currentTimeMillis()));
        return null;
        }, kafkaProducerHandlerPool);
        }, customPool))
        .toList();

        // Wait for all Kafka futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.debug("message=\"Publishing to Kafka for {}..time to publish message to topic= {} messages = {} ms, event = {} \"",
        context,
        Duration.between(publishStart, Instant.now()).toMillis(),
        eventsRetrieved.size());

        // Update events in database
        Instant updateStart = Instant.now();
        eventRepository.saveAll(eventsRetrieved);

        log.debug("message=\"Publishing to Kafka for {}..time to update event status= {} messages = {} ms, batch=\"",
        context,
        Duration.between(updateStart, Instant.now()).toMillis(),
        eventsRetrieved.size());
        }

/**
 * Heartbeat scheduler to maintain node presence
 */
@Scheduled(fixedRateString = "${outbox.task.scheduler.publish.multinode.heartbeat.interval:60000}")
public void maintainHeartbeat() {
        if (!multiNodeEnabled) {
        return;
        }

        try {
        // Update node heartbeat in ShedLock table
        updateNodeHeartbeatInShedLock();

        // Check for and pickup abandoned partitions
        pickupAbandonedPartitions();

        } catch (Exception ex) {
        log.error("Error in heartbeat maintenance for node: {}", nodeId, ex);
        }
        }

/**
 * Rebalance partitions when nodes join/leave
 * IMPROVED: Added proper locking and transaction management
 */
@Scheduled(fixedRateString = "${outbox.task.scheduler.publish.multinode.rebalance.interval:300000}") // 5 minutes
@SchedulerLock(name = "PARTITION_REBALANCER", lockAtLeastFor = "PT30S")
public void rebalancePartitions() {
        if (!multiNodeEnabled) {
        return;
        }

        try {
        List<String> activeNodes = getActiveNodesFromShedLock();
        rebalancePartitionsAcrossNodes(activeNodes);
        } catch (Exception ex) {
        log.error("Error in partition rebalancing", ex);
        }
        }

/**
 * Periodic cleanup of expired entries
 * NEW: Added cleanup scheduler
 */
@Scheduled(fixedRateString = "${outbox.task.scheduler.publish.multinode.cleanup.interval:86400000}") // Daily
public void cleanupExpiredEntries() {
        if (!multiNodeEnabled) {
        return;
        }

        try {
        shedLockRepository.deleteExpiredLocksByPrefix("NODE_HEARTBEAT_", Instant.now());
        log.info("Cleaned up expired node heartbeat entries");
        } catch (Exception ex) {
        log.error("Error during expired entries cleanup", ex);
        }
        }

/**
 * Graceful shutdown cleanup
 * NEW: Clean up resources on shutdown
 */
@PreDestroy
public void cleanupOnShutdown() {
        if (!multiNodeEnabled) {
        return;
        }

        try {
        // Release partitions owned by this node
        partitionAssignmentRepository.deleteByNodeId(nodeId);

        // Remove heartbeat
        shedLockRepository.deleteLock("NODE_HEARTBEAT_" + nodeId);

        log.info("Node {} cleaned up partitions and heartbeat on shutdown", nodeId);
        } catch (Exception ex) {
        log.warn("Error during graceful shutdown cleanup", ex);
        }
        }

/**
 * Register or update node in ShedLock table using a custom lock name
 */
private void registerNodeInShedLock() {
        String nodeLockName = "NODE_HEARTBEAT_" + nodeId;

        try {
        shedLockRepository.upsert(
        nodeLockName,
        Instant.now().plus(Duration.ofMillis(heartbeatInterval * 2)),
        Instant.now()
        );
        } catch (Exception ex) {
        log.debug("Node registration in ShedLock: {}", ex.getMessage());
        }
        }

/**
 * Update node heartbeat in ShedLock table
 */
private void updateNodeHeartbeatInShedLock() {
        String nodeLockName = "NODE_HEARTBEAT_" + nodeId;

        try {
        shedLockRepository.extend(
        nodeLockName,
        Instant.now().plus(Duration.ofMillis(heartbeatInterval * 2))
        );
        } catch (Exception ex) {
        // If extend fails, try to create new entry
        registerNodeInShedLock();
        }
        }

/**
 * Get list of active nodes from ShedLock table
 */
private List<String> getActiveNodesFromShedLock() {
        try {
        List<String> lockNames = shedLockRepository.findActiveLocksByPrefix("NODE_HEARTBEAT_", Instant.now());

        return lockNames.stream()
        .filter(lockName -> lockName.startsWith("NODE_HEARTBEAT_"))
        .map(lockName -> lockName.substring("NODE_HEARTBEAT_".length()))
        .collect(Collectors.toList());
        } catch (Exception ex) {
        log.error("Error getting active nodes from ShedLock", ex);
        return Collections.singletonList(nodeId);
        }
        }

/**
 * Get partitions assigned to this node
 */
private List<Integer> getAssignedPartitions() {
        return partitionAssignmentRepository.findPartitionsByNodeId(nodeId);
        }

/**
 * Rebalance partitions across active nodes
 * IMPROVED: Added transaction management for atomicity
 */
@Transactional
private void rebalancePartitionsAcrossNodes(List<String> activeNodes) {
        if (activeNodes.isEmpty()) {
        return;
        }

        int nodeCount = activeNodes.size();
        Map<String, List<Integer>> partitionAssignment = calculatePartitionDistribution(nodeCount);

        // Clear existing assignments and reassign atomically
        partitionAssignmentRepository.clearAllAssignments();

        // Assign partitions to nodes
        int nodeIndex = 0;
        for (String node : activeNodes) {
        List<Integer> partitions = partitionAssignment.get("node_" + nodeIndex);
        if (partitions != null && !partitions.isEmpty()) {
        assignPartitionsToNode(partitions, node);
        }
        nodeIndex++;
        }

        log.info("Rebalanced {} partitions across {} nodes", totalPartitions, nodeCount);
        }

/**
 * Calculate partition distribution based on strategy
 */
private Map<String, List<Integer>> calculatePartitionDistribution(int nodeCount) {
        Map<String, List<Integer>> distribution = new HashMap<>();

        // Initialize all node lists
        for (int i = 0; i < nodeCount; i++) {
        distribution.put("node_" + i, new ArrayList<>());
        }

        if (nodeCount >= totalPartitions) {
        // More nodes than partitions: 1:1 ratio with idle nodes
        for (int i = 0; i < totalPartitions; i++) {
        distribution.get("node_" + i).add(i);
        }
        } else {
        // More partitions than nodes: distribute evenly
        int partitionsPerNode = totalPartitions / nodeCount;
        int remainingPartitions = totalPartitions % nodeCount;

        int partitionIndex = 0;
        for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        int partitionsForThisNode = partitionsPerNode + (nodeIndex < remainingPartitions ? 1 : 0);

        for (int j = 0; j < partitionsForThisNode; j++) {
        distribution.get("node_" + nodeIndex).add(partitionIndex++);
        }
        }
        }

        return distribution;
        }

/**
 * Assign multiple partitions to node - batch operation with conflict handling
 * IMPROVED: Better conflict resolution
 */
private void assignPartitionsToNode(List<Integer> partitionIds, String nodeId) {
        for (Integer partitionId : partitionIds) {
        try {
        partitionAssignmentRepository.insertIfNotExists(partitionId, nodeId, new Timestamp(System.currentTimeMillis()));
        } catch (Exception ex) {
        // Partition already claimed by another node - skip silently
        log.debug("Partition {} already claimed during assignment to node {}", partitionId, nodeId);
        }
        }
        }

/**
 * Pick up abandoned partitions (partitions assigned to nodes that are no longer active)
 * IMPROVED: Enhanced abandoned partition detection with time-based checks
 */
private void pickupAbandonedPartitions() {
        List<String> activeNodes = getActiveNodesFromShedLock();
        Instant cutoffTime = Instant.now().minus(Duration.ofMillis(heartbeatInterval * 3));

        List<Integer> abandonedPartitions = partitionAssignmentRepository
        .findPartitionsAbandonedBefore(activeNodes, cutoffTime);

        if (!abandonedPartitions.isEmpty()) {
        log.info("Picking up abandoned partitions: {} by node: {}", abandonedPartitions, nodeId);

        // Remove abandoned assignments first
        partitionAssignmentRepository.deleteByPartitionIds(abandonedPartitions);

        // Then assign to current node
        assignPartitionsToNode(abandonedPartitions, nodeId);
        }
        }

/**
 * Generate unique node ID
 */
private String generateNodeId() {
        try {
        String hostName = InetAddress.getLocalHost().getHostName();
        String processId = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        return applicationName + "_" + hostName + "_" + processId + "_" + System.currentTimeMillis();
        } catch (Exception ex) {
        return applicationName + "_" + UUID.randomUUID().toString();
        }
        }

// IMPROVED Entity - Same structure but better repository methods
@Entity
@Table(name = "partition_assignments")
public class PartitionAssignment {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "partition_id", unique = true)
    private Integer partitionId;

    @Column(name = "node_id")
    private String nodeId;

    @Column(name = "assigned_time")
    private Timestamp assignedTime;

    // Getters and setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Integer getPartitionId() { return partitionId; }
    public void setPartitionId(Integer partitionId) { this.partitionId = partitionId; }

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public Timestamp getAssignedTime() { return assignedTime; }
    public void setAssignedTime(Timestamp assignedTime) { this.assignedTime = assignedTime; }
}

// IMPROVED Repository - Enhanced robustness with conflict handling
@Repository
public interface PartitionAssignmentRepository extends JpaRepository<PartitionAssignment, Long> {

    @Query("SELECT pa.partitionId FROM PartitionAssignment pa WHERE pa.nodeId = :nodeId")
    List<Integer> findPartitionsByNodeId(@Param("nodeId") String nodeId);

    @Query("SELECT pa.partitionId FROM PartitionAssignment pa WHERE pa.nodeId NOT IN :activeNodes")
    List<Integer> findPartitionsNotInNodeList(@Param("activeNodes") List<String> activeNodes);

    // NEW: Enhanced abandoned partition detection with time-based checks
    @Query("SELECT pa.partitionId FROM PartitionAssignment pa WHERE pa.nodeId NOT IN :activeNodes OR pa.assignedTime < :cutoffTime")
    List<Integer> findPartitionsAbandonedBefore(@Param("activeNodes") List<String> activeNodes, @Param("cutoffTime") Instant cutoffTime);

    // NEW: Conflict-safe insert using native query
    @Modifying
    @Query(value = "INSERT IGNORE INTO partition_assignments (partition_id, node_id, assigned_time) VALUES (?1, ?2, ?3)", nativeQuery = true)
    void insertIfNotExists(Integer partitionId, String nodeId, Timestamp assignedTime);

    @Modifying
    @Query("DELETE FROM PartitionAssignment")
    void clearAllAssignments();

    // NEW: Cleanup methods for graceful shutdown
    @Modifying
    @Query("DELETE FROM PartitionAssignment pa WHERE pa.nodeId = :nodeId")
    void deleteByNodeId(@Param("nodeId") String nodeId);

    @Modifying
    @Query("DELETE FROM PartitionAssignment pa WHERE pa.partitionId IN :partitionIds")
    void deleteByPartitionIds(@Param("partitionIds") List<Integer> partitionIds);
}

// EventRepository remains the same
public interface EventRepository extends JpaRepository<EventEntity, Long> {

    List<EventEntity> findUnpublishedEvents(int batchSize);

    @Query("SELECT e FROM EventEntity e WHERE e.publishedStatus = false AND e.partitionId IN :partitionIds ORDER BY e.createdTime ASC")
    List<EventEntity> findUnpublishedEventsByPartitions(@Param("partitionIds") List<Integer> partitionIds, Pageable pageable);

    default List<EventEntity> findUnpublishedEventsByPartitions(List<Integer> partitionIds, int batchSize) {
        return findUnpublishedEventsByPartitions(partitionIds, PageRequest.of(0, batchSize));
    }
}

// IMPROVED ShedLockRepository with additional methods
public interface ShedLockRepository {
    void upsert(String name, Instant lockUntil, Instant lockedAt);
    void extend(String name, Instant lockUntil);
    List<String> findActiveLocksByPrefix(String prefix, Instant currentTime);

    // NEW: Cleanup methods
    void deleteExpiredLocksByPrefix(String prefix, Instant currentTime);
    void deleteLock(String name);
}

/*
IMPROVED DATABASE SCHEMA:

-- Partition assignments table with proper indexes
CREATE TABLE partition_assignments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    partition_id INT NOT NULL UNIQUE,
    node_id VARCHAR(255) NOT NULL,
    assigned_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_partition_id (partition_id),
    INDEX idx_node_id (node_id),
    INDEX idx_assigned_time (assigned_time)
);

-- Events table (add partition_id if not present)
ALTER TABLE events ADD COLUMN partition_id INT;
CREATE INDEX idx_events_partition_id ON events(partition_id);
CREATE INDEX idx_events_published_partition ON events(published_status, partition_id);

IMPROVED CONFIGURATION (application.yml):
outbox:
  task:
    scheduler:
      publish:
        multinode:
          enabled: false  # Set to true to enable multi-node mode
          heartbeat:
            interval: 60000  # 1 minute
          total:
            partitions: 6
          rebalance:
            interval: 300000  # 5 minutes
          cleanup:
            interval: 86400000  # 24 hours

KEY IMPROVEMENTS MADE:
1. Fixed parallel processing: Per-node ShedLock names
2. Added transaction management for atomic rebalancing
3. Enhanced abandoned partition detection with time-based checks
4. Added graceful shutdown cleanup
5. Improved conflict handling in partition assignments
6. Added periodic cleanup of expired entries
7. Better error handling and logging
8. More robust repository methods with proper conflict resolution
*/