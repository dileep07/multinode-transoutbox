@Service
public class NodeRegistryService {
    
    private final NodeRepository nodeRepository;
    private final String nodeId;
    private static final Duration NODE_TIMEOUT = Duration.ofMinutes(2);
    
    public NodeRegistryService(NodeRepository nodeRepository, PartitionAssignmentService partitionAssignmentService) {
        this.nodeRepository = nodeRepository;
        this.nodeId = partitionAssignmentService.getNodeId();
    }
    
    /**
     * Update this node's heartbeat and clean expired nodes
     */
    public void updateNodeRegistry() {
        try {
            Instant now = Instant.now();
            
            // Update this node's heartbeat
            nodeRepository.upsertNode(nodeId, now);
            
            // Clean up expired nodes
            Instant cutoff = now.minus(NODE_TIMEOUT);
            int expiredCount = nodeRepository.deleteExpiredNodes(cutoff);
            
            Set<String> activeNodes = getActiveNodes();
            log.debug("Node registry updated. Current node: {}, Active nodes: {}, Expired nodes cleaned: {}", 
                    nodeId, activeNodes.size(), expiredCount);
            
        } catch (Exception e) {
            log.error("Failed to update node registry", e);
        }
    }
    
    /**
     * Get all currently active nodes
     */
    public Set<String> getActiveNodes() {
        try {
            Instant cutoff = Instant.now().minus(NODE_TIMEOUT);
            return nodeRepository.findActiveNodes(cutoff);
        } catch (Exception e) {
            log.error("Failed to get active nodes", e);
            // Return this node only as fallback
            return Set.of(nodeId);
        }
    }
    
    @EventListener
    public void handleApplicationReady(ApplicationReadyEvent event) {
        // Register this node immediately on startup
        log.info("Application ready, registering node: {}", nodeId);
        updateNodeRegistry();
    }
}