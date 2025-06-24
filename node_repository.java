@Repository
public interface NodeRepository extends JpaRepository<NodeEntity, String> {
    
    @Query("SELECT n.nodeId FROM NodeEntity n WHERE n.lastHeartbeat >= :cutoff")
    Set<String> findActiveNodes(@Param("cutoff") Instant cutoff);
    
    @Modifying
    @Transactional
    @Query("DELETE FROM NodeEntity n WHERE n.lastHeartbeat < :cutoff")
    int deleteExpiredNodes(@Param("cutoff") Instant cutoff);
    
    @Modifying
    @Transactional
    @Query(value = "INSERT INTO outbox_nodes (node_id, last_heartbeat) VALUES (:nodeId, :heartbeat) " +
                   "ON DUPLICATE KEY UPDATE last_heartbeat = :heartbeat", nativeQuery = true)
    void upsertNodeMySQL(@Param("nodeId") String nodeId, @Param("heartbeat") Instant heartbeat);
    
    @Modifying
    @Transactional
    @Query(value = "INSERT INTO outbox_nodes (node_id, last_heartbeat) VALUES (:nodeId, :heartbeat) " +
                   "ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = :heartbeat", nativeQuery = true)
    void upsertNodePostgreSQL(@Param("nodeId") String nodeId, @Param("heartbeat") Instant heartbeat);
    
    // Generic upsert method - implement based on your database
    default void upsertNode(String nodeId, Instant heartbeat) {
        Optional<NodeEntity> existing = findById(nodeId);
        if (existing.isPresent()) {
            NodeEntity node = existing.get();
            node.setLastHeartbeat(heartbeat);
            save(node);
        } else {
            NodeEntity newNode = new NodeEntity();
            newNode.setNodeId(nodeId);
            newNode.setLastHeartbeat(heartbeat);
            save(newNode);
        }
    }
}

// NodeEntity class
@Entity
@Table(name = "outbox_nodes")
public class NodeEntity {
    @Id
    @Column(name = "node_id")
    private String nodeId;
    
    @Column(name = "last_heartbeat")
    private Instant lastHeartbeat;
    
    @Column(name = "created_at")
    private Instant createdAt = Instant.now();
    
    // getters and setters
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public Instant getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(Instant lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}