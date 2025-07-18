package org.example.workers

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.example.services.DbPersistenceService
import org.example.services.InMemoryService
import org.example.services.TransformService
import org.example.workers.dbPersistence.DbPersistenceWorker
import java.util.concurrent.ConcurrentHashMap

class KafkaPartitionProcessor(
    private val inMemoryService: InMemoryService,
    private val dbService: DbPersistenceService,
    private val transformService: TransformService,
) {
    private val processorScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val partitionMutexes = ConcurrentHashMap<TopicPartition, Mutex>()

    private val dbWorkers = ConcurrentHashMap<TopicPartition, DbPersistenceWorker>()

    fun submit(partition: TopicPartition, records: List<ConsumerRecord<String, String>>) {
        val mutex = partitionMutexes.computeIfAbsent(partition) { Mutex() }
        // Get or create the dedicated worker for this partition.
        val dbWorker = dbWorkers.computeIfAbsent(partition) {
            DbPersistenceWorker(it, dbService, transformService).also { worker ->
                worker.start() // Start its background processing loop.
            }
        }
        processorScope.launch {
            mutex.withLock {
                // For the in-memory view, iterate and launch a separate coroutine for each record.
                // This allows for immediate processing of each message for the UI/client without waiting for the batch.
                records.forEach { record ->

                    dbWorker.submitRecord(record)

                    launch(Dispatchers.Default) {
                        val whiteboardId = record.key() ?: "unknown-${partition.partition()}"
                        "State from offset ${record.offset()}"
                        val message = record.value()
                        inMemoryService.updateWhiteboardState(whiteboardId, message)
                        println("QUERY worker for partition [${partition.partition()}] processed single record at offset ${record.offset()}.")
                    }
                }
            }
        }
    }

    fun shutdown() {
        println("Shutting down all partition processors by cancelling the scope...")
        // Gracefully shut down each individual DB worker.
        dbWorkers.values.forEach { it.shutdown() }
        dbWorkers.clear()

        processorScope.cancel() // This cancels all running coroutines.
        println("All partition processors shut down.")
    }
}