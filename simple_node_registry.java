@Service
public class NodeRegistryService {

    private static final String NODE_HEARTBEAT_PREFIX = "node-heartbeat-";
    private static final Duration NODE_TIMEOUT = Duration.ofMinutes(2);

    private final ShedLockTemplate shedLockTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final String nodeId;

    public NodeRegistryService(
            ShedLockTemplate shedLockTemplate,
            JdbcTemplate jdbcTemplate,
            PartitionAssignmentService partitionAssignmentService) {
        this.shedLockTemplate = shedLockTemplate;
        this.jdbcTemplate = jdbcTemplate;
        this.nodeId = partitionAssignmentService.getNodeId();
    }

    /**
     * Update this node's heartbeat using ShedLock
     */
    public void updateNodeHeartbeat() {
        String lockName = NODE_HEARTBEAT_PREFIX + nodeId;

        try {
            LockConfiguration lockConfig = new LockConfiguration(
                    Instant.now(),
                    lockName,
                    NODE_TIMEOUT,         // This node is considered alive for 2 minutes
                    Duration.ofSeconds(1) // Minimum lock duration
            );

            shedLockTemplate.executeWithLock(() -> {
                // The lock acquisition itself serves as the heartbeat
                // locked_at = current time, locked_until = current time + 2 minutes
                log.debug("Heartbeat updated for node: {}", nodeId);
            }, lockConfig);

        } catch (Exception e) {
            log.error("Failed to update heartbeat for node: {}", nodeId, e);
        }
    }

    /**
     * Get all currently active nodes by querying ShedLock table
     */
    public Set<String> getActiveNodes() {
        try {
            String sql = """
                SELECT name FROM shedlock 
                WHERE name LIKE ? 
                AND locked_until > ? 
                ORDER BY name
                """;

            Instant now = Instant.now();
            String pattern = NODE_HEARTBEAT_PREFIX + "%";

            List<String> lockNames = jdbcTemplate.queryForList(
                    sql, String.class, pattern, Timestamp.from(now)
            );

            // Extract node IDs from lock names
            Set<String> activeNodes = lockNames.stream()
                    .map(lockName -> lockName.substring(NODE_HEARTBEAT_PREFIX.length()))
                    .collect(Collectors.toSet());

            log.debug("Found {} active nodes: {}", activeNodes.size(), activeNodes);
            return activeNodes;

        } catch (Exception e) {
            log.error("Failed to get active nodes, falling back to single node", e);
            // Fallback to single node operation
            return Set.of(nodeId);
        }
    }

    /**
     * Clean up expired heartbeat locks (optional, ShedLock may handle this)
     */
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void cleanupExpiredHeartbeats() {
        try {
            String sql = """
                DELETE FROM shedlock 
                WHERE name LIKE ? 
                AND locked_until < ?
                """;

            Instant cutoff = Instant.now().minus(Duration.ofMinutes(5));
            String pattern = NODE_HEARTBEAT_PREFIX + "%";

            int deleted = jdbcTemplate.update(sql, pattern, Timestamp.from(cutoff));

            if (deleted > 0) {
                log.debug("Cleaned up {} expired node heartbeats", deleted);
            }

        } catch (Exception e) {
            log.error("Failed to cleanup expired heartbeats", e);
        }
    }

    @EventListener
    public void handleApplicationReady(ApplicationReadyEvent event) {
        log.info("Application ready, registering node heartbeat: {}", nodeId);
        updateNodeHeartbeat();
    }

    @PreDestroy
    public void handleShutdown() {
        log.info("Application shutting down, cleaning up node heartbeat: {}", nodeId);
        try {
            // Remove our heartbeat lock on shutdown
            String lockName = NODE_HEARTBEAT_PREFIX + nodeId;
            jdbcTemplate.update("DELETE FROM shedlock WHERE name = ?", lockName);
        } catch (Exception e) {
            log.warn("Failed to cleanup heartbeat on shutdown", e);
        }
    }
}