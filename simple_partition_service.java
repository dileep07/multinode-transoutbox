@Service
public class PartitionAssignmentService {
    
    private final String nodeId;
    private final int totalPartitions;
    
    public PartitionAssignmentService(
            @Value("${outbox.node.id:#{T(java.util.UUID).randomUUID().toString()}}") String nodeId,
            @Value("${outbox.total.partitions:6}") int totalPartitions) {
        this.nodeId = nodeId;
        this.totalPartitions = totalPartitions;
        log.info("Initialized PartitionAssignmentService - NodeId: {}, TotalPartitions: {}", nodeId, totalPartitions);
    }
    
    /**
     * Get partitions assigned to this node based on active nodes
     */
    public Set<Integer> getAssignedPartitions(Set<String> activeNodes) {
        if (activeNodes.isEmpty() || !activeNodes.contains(nodeId)) {
            log.warn("Node {} not found in active nodes: {}", nodeId, activeNodes);
            return Collections.emptySet();
        }
        
        List<String> sortedNodes = new ArrayList<>(activeNodes);
        Collections.sort(sortedNodes); // Ensure consistent ordering across all nodes
        
        int nodeIndex = sortedNodes.indexOf(nodeId);
        Set<Integer> assignedPartitions = new HashSet<>();
        
        // Distribute partitions evenly using round-robin
        for (int partition = 0; partition < totalPartitions; partition++) {
            if (partition % sortedNodes.size() == nodeIndex) {
                assignedPartitions.add(partition);
            }
        }
        
        log.debug("Node {} (index {}) assigned partitions: {} from total nodes: {}", 
                nodeId, nodeIndex, assignedPartitions, sortedNodes.size());
        
        return assignedPartitions;
    }
    
    /**
     * Calculate partition for a message (useful when creating outbox events)
     */
    public int calculatePartition(String key) {
        return Math.abs(key.hashCode()) % totalPartitions;
    }
    
    public String getNodeId() {
        return nodeId;
    }
    
    public int getTotalPartitions() {
        return totalPartitions;
    }
}