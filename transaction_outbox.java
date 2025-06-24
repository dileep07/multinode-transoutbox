public class TransactionOutbox {
    
    private final EventRepository eventRepository;
    private final ExecutorService customPool;
    private final ExecutorService kafkaProducerHandlerPool;
    private final PartitionAssignmentService partitionAssignmentService;
    private final NodeRegistryService nodeRegistryService;
    
    // Add these new dependencies to your constructor
    
    /**
     * Node registry heartbeat - keeps track of active nodes
     */
    @Scheduled(fixedRateString = "${outbox.task.scheduler.heartbeat.fixedRate:30000}")
    @SchedulerLock(name = "outbox.task.heartbeat.name:NodeHeartbeat", lockAtLeastFor = "${outbox.task.scheduler.heartbeat.lockAtLeastFor:PT29S}")
    public void updateNodeHeartbeat() {
        nodeRegistryService.updateNodeRegistry();
    }
    
    /**
     * Modified main processing method - now processes assigned partitions only
     */
    @Scheduled(fixedRateString = "${outbox.task.scheduler.publish.fixedRate:10000}", initialDelayString = "${outbox.task.scheduler.publish.initialDelay:10000}")
    public void publishToKafka() {
        // Get active nodes and determine assigned partitions
        Set<String> activeNodes = nodeRegistryService.getActiveNodes();
        Set<Integer> assignedPartitions = partitionAssignmentService.getAssignedPartitions(activeNodes);
        
        if (assignedPartitions.isEmpty()) {
            log.debug("No partitions assigned to node: {}", partitionAssignmentService.getNodeId());
            return;
        }
        
        log.debug("Node {} processing partitions: {}", 
                partitionAssignmentService.getNodeId(), assignedPartitions);
        
        // Process each assigned partition with its own lock
        assignedPartitions.forEach(this::processPartition);
    }
    
    private void processPartition(Integer partitionId) {
        String lockName = "outbox.task.partition." + partitionId;
        
        try {
            // Use ShedLock for each partition separately
            LockConfiguration lockConfig = new LockConfiguration(
                Instant.now(), 
                lockName,
                Duration.ofMinutes(5), // lockAtMostFor
                Duration.ofSeconds(30)  // lockAtLeastFor
            );
            
            shedLockTemplate.executeWithLock(() -> {
                doProcessPartition(partitionId);
            }, lockConfig);
            
        } catch (Exception e) {
            log.error("Failed to acquire lock or process partition: {}", partitionId, e);
        }
    }
    
    private void doProcessPartition(Integer partitionId) {
        // Retrieve unpublished events for this specific partition
        Instant queryStart = Instant.now();
        List<EventEntity> eventsRetrieved = eventRepository.findUnpublishedEventsByPartition(partitionId, batchSize);
        
        log.debug("Partition {}: Retrieved {} events in {} ms", 
                partitionId, eventsRetrieved.size(),
                Duration.between(queryStart, Instant.now()).toMillis());
        
        if (eventsRetrieved.isEmpty()) {
            return;
        }
        
        eventsRetrieved.forEach(this::processEvent);
    }
    
    private void processEvent(EventEntity eventRetrieved) {
        Instant publishStart = Instant.now();
        
        List<CompletableFuture<Void>> futures = eventsRetrieved.stream()
                .map(eventRetrieved -> CompletableFuture.runAsync(() -> {
                    try {
                        log.debug("Started async processing for event: {}, Thread: {}", 
                                eventRetrieved.getId(), Thread.currentThread().getName());
                        
                        byte[] payload = eventRetrieved.getPayload();
                        byte[] key = eventRetrieved.getKey();
                        
                        kafkaTemplate.send(eventRetrieved.getTopic(), key, payload)
                                .thenAcceptAsync(sendResult -> {
                                    log.debug("Successfully published event: {}", eventRetrieved.getId());
                                    eventRetrieved.setPublishedTime(new Timestamp(sendResult.getRecordMetadata().timestamp()));
                                    eventRetrieved.setPublishedStatus(true);
                                    eventRetrieved.setAckTime(new Timestamp(System.currentTimeMillis()));
                                    eventRetrieved.setPartitionId(String.valueOf(sendResult.getRecordMetadata().partition()));
                                }, kafkaProducerHandlerPool)
                                .exceptionallyAsync(throwable -> {
                                    log.error("Error publishing event: {}", eventRetrieved.getId(), throwable);
                                    return null;
                                });
                                
                    } catch (Exception ex) {
                        log.error("Error processing event: {}", eventRetrieved.getId(), ex);
                    }
                }, customPool))
                .collect(Collectors.toList());
        
        // Wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
}