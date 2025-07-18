package org.example.workers.dbPersistence

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.example.services.DbPersistenceService
import org.example.services.TransformService
import java.util.concurrent.atomic.AtomicBoolean


class DbPersistenceWorker
    (
    private val partition: TopicPartition,
    private val dbService: DbPersistenceService,
    private val transformService: TransformService
) {

    // The BatchStorage is now part of the worker's scope.
    private val batchStorage = BatchStorage<ConsumerRecord<String, String>>()
    private val workerScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val running = AtomicBoolean(false)


    /**
     * Starts the worker's internal processing loop in the background.
     * This loop periodically checks for and processes ready batches.
     */
    fun start() {
        if (running.compareAndSet(false, true)) {
            println("💾 Starting DB Persistence Worker for partition ${partition.partition()}")
            workerScope.launch {
                while (running.get()) {
                    if (batchStorage.isBatchReady()) {
                        processNextBatch()
                    }
                    // Wait a bit before checking again to avoid a busy-loop.
                    delay(100)
                }
            }
        }
    }

    /**
     * Public method to add a record to this worker's batching queue.
     */
    fun submitRecord(record: ConsumerRecord<String, String>) {
        batchStorage.add(record)
    }

    private fun processNextBatch() {
        try {
            val batch = batchStorage.getBatch()
            println("COMMAND worker for partition [${partition.partition()}] processing a batch of ${batch.size} records.")
            batch.forEach { record ->
                performOperationalTransformation(record)
                println("[DB service] SAVE record")
            }
            println("[DB service] SAVE batch and offset")
            println("COMMAND worker for partition [${partition.partition()}] finished batch.")
        } catch (e: NoSuchElementException) {
            // This is expected if another thread processes the last batch first.
        } catch (e: Exception) {
            System.err.println("Error processing batch for partition ${partition.partition()}: ${e.message}")
        }
    }


    private fun performOperationalTransformation(record: ConsumerRecord<String, String>) {
        println("  [Partition-${partition.partition()}] Applying OT on offset ${record.offset()} for DB...")
        try {
            record.value()
            transformService.transform()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    /**
     * Gracefully shuts down the worker, ensuring any remaining records are processed.
     */
    fun shutdown() {
        if (running.compareAndSet(true, false)) {
            println("Shutting down DB worker for partition ${partition.partition()}...")
            // Flush any lingering records and process the final batch.
            batchStorage.flush()
            if (batchStorage.isBatchReady()) {
                processNextBatch()
            }
            workerScope.cancel()
            println("DB worker for partition ${partition.partition()} shut down.")
        }
    }
}
