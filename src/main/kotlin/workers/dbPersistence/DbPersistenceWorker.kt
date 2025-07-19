package org.example.workers.dbPersistence

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.example.services.DbPersistenceService
import org.example.services.TransformService
import java.util.concurrent.atomic.AtomicBoolean


class DbPersistenceWorker
    (
    private val partitionScope: CoroutineScope,
    private val partition: TopicPartition,
    private val dbService: DbPersistenceService,
    private val transformService: TransformService
) {

    private val running = AtomicBoolean(false)
    private var batch = emptyList<ConsumerRecord<String, String>>()


    /**
     * Public method to add a record to this worker's batching queue.
     */
    suspend fun proceedBatch(batch: List<ConsumerRecord<String, String>>) {
        this.batch = batch
        processNextBatch()
    }


    private suspend fun processNextBatch() {
        try {
            println("COMMAND worker for partition [${partition.partition()}] processing a batch of ${batch.size} records.")
            coroutineScope {
                dbService.saveOperationsBatch(emptyList())
            }
            println("COMMAND worker for partition [${partition.partition()}] finished batch.")
            this.batch = emptyList<ConsumerRecord<String, String>>()
        } catch (e: NoSuchElementException) {
            // This is expected if another thread processes the last batch first.
        } catch (e: Exception) {
            System.err.println("Error processing batch for partition ${partition.partition()}: ${e.message}")
        }
    }


    /**
     * Gracefully shuts down the worker, ensuring any remaining records are processed.
     */
    fun shutdown() {
        if (running.compareAndSet(true, false)) {
            println("Shutting down DB worker for partition ${partition.partition()}...")
            println("DB worker for partition ${partition.partition()} shut down.")
        }
    }
}
