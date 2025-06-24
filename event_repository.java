@Repository
public interface EventRepository extends JpaRepository<EventEntity, Long> {
    
    // Your existing method - keep this for backward compatibility
    @Query("SELECT e FROM EventEntity e WHERE e.publishedStatus = false ORDER BY e.createdTime ASC")
    List<EventEntity> findUnpublishedEvents(Pageable pageable);
    
    // New method - find unpublished events by partition
    @Query("SELECT e FROM EventEntity e WHERE e.publishedStatus = false AND e.partitionId = :partitionId ORDER BY e.createdTime ASC")
    List<EventEntity> findUnpublishedEventsByPartition(@Param("partitionId") Integer partitionId, Pageable pageable);
    
    // Convenience method with limit
    default List<EventEntity> findUnpublishedEventsByPartition(Integer partitionId, int limit) {
        return findUnpublishedEventsByPartition(partitionId, PageRequest.of(0, limit));
    }
    
    // Optional: Method to get count by partition for monitoring
    @Query("SELECT COUNT(e) FROM EventEntity e WHERE e.publishedStatus = false AND e.partitionId = :partitionId")
    long countUnpublishedEventsByPartition(@Param("partitionId") Integer partitionId);
}