package org.example.workers

import com.fasterxml.jackson.module.kotlin.readValue
import org.example.data.*
import org.example.services.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

// Kafka constants for the application
private const val KAFKA_TOPIC = "whiteboard_operations"
private const val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092" // Ensure your Kafka instance is running here

// --- In-Memory Worker Coroutine ---
// Consumes Kafka messages, applies OT, updates in-memory cache, and publishes to Redis.
class InMemoryWorker(
    private val consumerGroupId: String = "in_memory_group" // Unique consumer group ID
) {
    private val running = AtomicBoolean(true) // Flag to control the consumer loop

    // Starts the worker as a long-running coroutine in the provided scope.
    // Uses Dispatchers.IO for blocking Kafka operations.
    fun start(scope: CoroutineScope) = scope.launch(Dispatchers.IO) {
        println("🚀 Starting In-Memory Worker with Group ID: '$consumerGroupId'")

        // Configure Kafka consumer properties
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS)
            put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // Start from earliest offset if no committed offset exists
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // Disable auto-commit; we'll commit manually
        }

        // Use 'use' block for auto-closing the Kafka consumer
        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(KAFKA_TOPIC)) // Subscribe to the whiteboard operations topic

            // --- Initial catch-up logic (part of initialization) ---
            // When the app starts, this worker needs to catch up its in-memory state.
            // Kafka's auto_offset_reset and group management handle the initial offset.
            // If you needed to explicitly seek to a *specific* offset from an external source (e.g., a snapshot's offset),
            // you'd do it here after consumer.assignment() is stable:
            // consumer.assignment().forEach { tp -> consumer.seek(tp, someSpecificOffset) }
            // For now, we let Kafka manage the starting point based on committed offsets.

            while (running.get()) { // Main consumer polling loop
                try {
                    // Poll for records from Kafka with a timeout.
                    val records = consumer.poll(Duration.ofMillis(100))
                    if (!records.isEmpty) {
                        println("[In-Memory Worker] Polled ${records.count()} records.")
                        for (record in records) {
                            val message = objectMapper.readValue<WhiteboardOperationMessage>(record.value())
                            val whiteboardId = record.key() ?: message.whiteboardId // Prefer key if available, else from message

                            // --- Apply Operational Transformation and update in-memory cache ---
                            // This is the core logic for the in-memory worker.
                            val success = InMemoryWhiteboardCache.applyOperation(whiteboardId, message)

                            if (success) {
                                // --- Publish transformed operation to Redis ---
                                RedisPublisher.publishTransformedOperation(message)

                                // --- Commit offset immediately after successful processing and Redis publishing ---
                                // For "as soon as possible", commit offset for the *next* message to be read (+1).
                                // It's important that your OT and Redis publishing are IDEMPOTENT.
                                consumer.commitSync(mapOf(
                                    TopicPartition(record.topic(), record.partition()) to OffsetAndMetadata(record.offset() + 1)
                                ))
                                println("[In-Memory Worker] Committed offset ${record.offset() + 1} for partition ${record.partition()}.")
                            } else {
                                // If OT application fails (e.g., conflict not resolved, invalid operation),
                                // we do NOT commit the offset, so this message will be re-processed.
                                println("[In-Memory Worker] Failed to apply operation '${message.operationId}'. Not committing offset. Message will be re-processed.")
                                // Depending on your error handling, you might also log to a dead-letter topic or retry.
                            }
                        }
                    }
                } catch (e: WakeupException) {
                    // Expected exception when consumer.wakeup() is called for shutdown.
                    println("[In-Memory Worker] Received WakeupException. Shutting down.")
                    running.set(false) // Ensure loop exits
                } catch (e: Exception) {
                    // Catch any other unexpected exceptions during polling or processing.
                    System.err.println("[In-Memory Worker] Unhandled error: ${e.message}")
                    e.printStackTrace()
                    // Notify engineers about critical errors.
                    NotificationService.sendAlert("In-Memory Worker critical error: ${e.message}")
                    delay(5000) // Small delay before retrying poll to prevent tight loop on persistent errors.
                }
            }
            println("🛑 In-Memory Worker stopped.")
        }
    }

    // Signals the worker to stop its polling loop gracefully.
    fun stop() {
        running.set(false)
        // If a consumer is currently blocked on poll(), wakeup() will interrupt it.
        // Needs access to the consumer instance, which is tricky with `use` block.
        // A common pattern is to store the consumer in a nullable var or pass it.
        // For simplicity, we rely on the loop's check.
    }
}

// --- DB Persistence Worker Coroutine ---
// Consumes Kafka messages, batches them, and persists them to the database.
class DbPersistenceWorker(
    private val consumerGroupId: String = "persistence_group", // Unique consumer group ID
    private val batchSize: Int = 1000 // Number of messages to batch before persisting
) {
    private val running = AtomicBoolean(true) // Flag to control the consumer loop
    private val batchBuffer = ConcurrentLinkedQueue<PersistedOperation>() // Thread-safe buffer for messages
    // Stores the latest offset for each partition in the current batch.
    private val currentOffsets = ConcurrentHashMap<TopicPartition, OffsetAndMetadata>()

    // Starts the worker as a long-running coroutine in the provided scope.
    // Uses Dispatchers.IO for blocking Kafka operations and database I/O.
    fun start(scope: CoroutineScope) = scope.launch(Dispatchers.IO) {
        println("💾 Starting DB Persistence Worker with Group ID: '$consumerGroupId'")

        // Configure Kafka consumer properties
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS)
            put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name) // Use StringDeserializer
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false") // Disable auto-commit
        }

        KafkaConsumer<String, String>(props).use { consumer ->
            consumer.subscribe(listOf(KAFKA_TOPIC)) // Subscribe to the topic

            // --- Startup: Seek to last committed offset from persistence DB ---
            // This is a crucial part of the application's initialization sequence.
            // It ensures the persistence worker starts consuming from the correct point.
            // This loop ensures seeking for all assigned partitions.
            consumer.assignment().forEach { topicPartition ->
                // Load last offset for this partition from your external persistence database
                val lastSavedOffset = PersistenceDatabase.loadLastOffset(consumerGroupId, topicPartition.topic(), topicPartition.partition())
                if (lastSavedOffset != null) {
                    consumer.seek(topicPartition, lastSavedOffset.offset)
                    println("[DB Persistence Worker] Seeked partition ${topicPartition.partition()} to offset ${lastSavedOffset.offset}")
                }
            }
            // A small poll after seeking is sometimes beneficial to ensure consumer state is updated.
            consumer.poll(Duration.ofMillis(0))


            // Launch a coroutine for periodic snapshotting (Step 3 in initialization - save newest document)
            // This handles saving the *current state* from the in-memory cache to persistence DB.
            scope.launch {
                while(running.get()) {
                    delay(30_000L) // Snapshot every 30 seconds (adjust as needed)
                    // Get all whiteboard IDs that have changed or need periodic snapshotting
                    val whiteboardIdsToSnapshot = InMemoryWhiteboardCache.whiteboards.keys
                    for (whiteboardId in whiteboardIdsToSnapshot) {
                        val state = InMemoryWhiteboardCache.getWhiteboardState(whiteboardId)
                        if (state != null) {
                            // Create snapshot, you might need the last processed offset for this whiteboard from the in-memory worker
                            // For simplicity, we just take the current state and a dummy offset or a high watermark from in-memory's commits.
                            // Realistically, coordinating the *exact* offset for a snapshot across two groups is very complex.
                            val snapshot = PersistedWhiteboardSnapshot(
                                whiteboardId = state.whiteboardId,
                                title = state.title,
                                status = state.status,
                                rootComponentJson = objectMapper.writeValueAsString(state.rootComponent),
                                elementsJson = objectMapper.writeValueAsString(state.elements),
                                lastKafkaOffset = 0, // Placeholder, see note above. In practice, derive from in-memory's internal offset tracking or a global watermark.
                                snapshotTimestamp = Instant.now()
                            )
                            PersistenceDatabase.saveWhiteboardSnapshot(snapshot)
                        }
                    }
                }
            }

            while (running.get()) { // Main consumer polling loop
                try {
                    val records = consumer.poll(Duration.ofMillis(100))
                    if (!records.isEmpty) {
                        println("[DB Persistence Worker] Polled ${records.count()} records.")
                        for (record in records) {
                            val message = objectMapper.readValue<WhiteboardOperationMessage>(record.value())
                            val opToPersist = PersistedOperation(
                                whiteboardId = record.key() ?: message.whiteboardId,
                                sessionId = message.sessionId,
                                operationId = message.operationId,
                                timestamp = message.timestamp,
                                operationType = message.operationType.name,
                                payloadJson = record.value(), // Store raw JSON payload
                                kafkaTopic = record.topic(),
                                kafkaPartition = record.partition(),
                                kafkaOffset = record.offset()
                            )
                            batchBuffer.add(opToPersist) // Add to the buffer
                            // Update the highest offset seen for this partition in the current batch
                            currentOffsets[TopicPartition(record.topic(), record.partition())] = OffsetAndMetadata(record.offset() + 1)
                        }

                        // If batch size is reached, process the batch
                        if (batchBuffer.size >= batchSize) {
                            processBatch(consumer)
                        }
                    }
                    // Implement a timeout to process smaller batches if batchSize is not met quickly.
                    // This could be a separate coroutine or a check every few polls.
                } catch (e: WakeupException) {
                    // Expected exception on shutdown
                    println("[DB Persistence Worker] Received WakeupException. Shutting down.")
                    running.set(false)
                } catch (e: Exception) {
                    System.err.println("[DB Persistence Worker] Unhandled error: ${e.message}")
                    e.printStackTrace()
                    NotificationService.sendAlert("DB Persistence Worker critical error: ${e.message}")
                    delay(5000)
                }
            }
            // Process any remaining messages in the buffer before final shutdown.
            if (batchBuffer.isNotEmpty()) {
                processBatch(consumer)
            }
            println("🛑 DB Persistence Worker stopped.")
        }
    }

    // Processes the buffered batch of operations.
    private suspend fun processBatch(consumer: KafkaConsumer<String, String>) {
        if (batchBuffer.isEmpty()) return

        val batchToProcess = mutableListOf<PersistedOperation>()
        // Drain the buffer up to batchSize
        while (batchBuffer.isNotEmpty() && batchToProcess.size < batchSize) {
            batchBuffer.poll()?.let { batchToProcess.add(it) }
        }

        if (batchToProcess.isEmpty()) return

        val success = PersistenceDatabase.saveOperationsBatch(batchToProcess) // Attempt to save to DB
        if (success) {
            // --- Commit offsets to Kafka ---
            // Only commit to Kafka if the database write was successful.
            consumer.commitSync(currentOffsets)
            println("[DB Persistence Worker] Committed ${batchToProcess.size} records and offsets to Kafka for current batch.")

            // --- Store latest offset to persistence database after successful transaction ---
            // This is your external offset tracking.
            // Iterate over all partitions for which offsets were updated in this batch
            currentOffsets.forEach { (tp, om) ->
                PersistenceDatabase.saveLastOffset(LastCommittedOffset(consumerGroupId, tp.topic(), tp.partition(), om.offset()))
            }
            currentOffsets.clear() // Clear offsets that have been successfully committed and saved
        } else {
            // --- Failure handling: If DB write fails, do NOT commit Kafka offset. ---
            NotificationService.sendAlert("DB Persistence Worker: Batch save failed. Messages will be re-processed on next poll. Batch size: ${batchToProcess.size}")
            // Re-add messages to buffer to be retried on the next `processBatch` call
            batchToProcess.forEach { batchBuffer.offer(it) }
        }
    }

    // Signals the worker to stop its polling loop gracefully.
    fun stop() {
        running.set(false)
    }
}
