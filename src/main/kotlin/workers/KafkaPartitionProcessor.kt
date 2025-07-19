package org.example.workers

import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.example.data.redis.RedisPublisher
import org.example.services.DbPersistenceService
import org.example.services.InMemoryService
import org.example.services.TransformService
import org.example.workers.dbPersistence.DbPersistenceWorker
import org.example.workers.inMemory.InMemoryWorker
import java.util.concurrent.ConcurrentHashMap

class KafkaPartitionProcessor(
    private val inMemoryService: InMemoryService,
    private val dbService: DbPersistenceService,
    private val transformService: TransformService,
) {
    private val partitionScopePool = ConcurrentHashMap<TopicPartition, CoroutineScope>()
    private val dbWorkers = ConcurrentHashMap<TopicPartition, DbPersistenceWorker>()
    private val inMemoryWorkers = ConcurrentHashMap<TopicPartition, InMemoryWorker>()

    fun submit(partition: TopicPartition, records: List<ConsumerRecord<String, String>>) {
        val partitionScope =
            partitionScopePool.computeIfAbsent(partition) { CoroutineScope(SupervisorJob() + Dispatchers.IO) }

        val dbWorker = dbWorkers.computeIfAbsent(partition) {
            DbPersistenceWorker(partitionScope, it, dbService, transformService)
        }

        val inMemoryWorker = inMemoryWorkers.computeIfAbsent(partition) {
            InMemoryWorker(partitionScope, partition, inMemoryService, transformService, RedisPublisher) {
                if (it.isNotEmpty()) {
                    partitionScope.launch {
                        dbWorker.proceedBatch(it)
                    }
                }
            }.also {
                it.start()
            }
        }

        partitionScope.launch {
            val batch = inMemoryWorker.proceedRecords(records)
            if (batch.isNotEmpty()) {
                dbWorker.proceedBatch(batch)
            }
        }
    }

    fun shutdown() {
        println("Shutting down all partition processors by cancelling the scope...")
        // Gracefully shut down each individual DB worker.
        dbWorkers.values.forEach { it.shutdown() }
        dbWorkers.clear()

        inMemoryWorkers.values.forEach { it.shutdown() }
        inMemoryWorkers.clear()

        partitionScopePool.values.forEach {
            it.cancel()
        }
        partitionScopePool.clear()

        println("All partition processors shut down.")
    }
}