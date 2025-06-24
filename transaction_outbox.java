public class TransactionOutbox {

    private final EventRepository eventRepository;
    private final ExecutorService customPool;
    private final ExecutorService kafkaProducerHandlerPool;
    private final PartitionAssignmentService partitionAssignmentService;
    private final NodeRegistryService nodeRegistryService;
    private final ShedLockTemplate shedLockTemplate;

    /**
     * Node heartbeat - registers this node as active using ShedLock
     */
    @Scheduled(fixedRateString = "${outbox.task.scheduler.heartbeat.fixedRate:30000}")
    public void updateNodeHeartbeat() {
        nodeRegistryService.updateNodeHeartbeat();
    }

    /**
     * Main processing method - processes assigned partitions only
     */
    @Scheduled(fixedRateString = "${outbox.task.scheduler.publish.fixedRate:10000}",
            initialDelayString = "${outbox.task.scheduler.publish.initialDelay:10000}")
    public void publishToKafka() {
        // Get active nodes from ShedLock table
        Set<String> activeNodes = nodeRegistryService.getActiveNodes();
        Set<Integer> assignedPartitions = partitionAssignmentService.getAssignedPartitions(activeNodes);

        if (assignedPartitions.isEmpty()) {
            log.debug("No partitions assigned to node: {}", partitionAssignmentService.getNodeId());
            return;
        }

        log.debug("Node {} processing partitions: {} (total active nodes: {})",
                partitionAssignmentService.getNodeId(), assignedPartitions, activeNodes.size());

        // Process each assigned partition with its own ShedLock
        assignedPartitions.forEach(this::processPartition);
    }

    private void processPartition(Integer partitionId) {
        String lockName = "outbox-partition-" + partitionId;

        try {
            LockConfiguration lockConfig = new LockConfiguration(
                    Instant.now(),
                    lockName,
                    Duration.ofMinutes(5),  // lockAtMostFor - partition processing timeout
                    Duration.ofSeconds(30)  // lockAtLeastFor - minimum processing time
            );

            shedLockTemplate.executeWithLock(() -> {
                doProcessPartition(partitionId);
            }, lockConfig);

        } catch (LockException e) {
            // Another node is processing this partition, which is expected
            log.debug("Partition {} already being processed by another node", partitionId);
        } catch (Exception e) {
            log.error("Failed to process partition: {}", partitionId, e);
        }
    }

    private void doProcessPartition(Integer partitionId) {
        Instant queryStart = Instant.now();

        // Your existing query logic, but filtered by partition
        List<EventEntity> eventsRetrieved = eventRepository
                .findUnpublishedEventsByPartition(partitionId, batchSize);

        log.debug("Partition {}: Retrieved {} events in {} ms",
                partitionId, eventsRetrieved.size(),
                Duration.between(queryStart, Instant.now()).toMillis());

        if (eventsRetrieved.isEmpty()) {
            return;
        }

        // Your existing Kafka publishing logic
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
                                    // Note: Kafka partition != outbox partition
                                    eventRetrieved.setKafkaPartition(String.valueOf(sendResult.getRecordMetadata().partition()));
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

        log.debug("Partition {}: Completed processing {} events in {} ms",
                partitionId, eventsRetrieved.size(),
                Duration.between(publishStart, Instant.now()).toMillis());
    }
}