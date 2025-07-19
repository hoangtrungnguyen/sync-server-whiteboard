package org.example.workers.inMemory

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.example.data.redis.RedisPublisher
import org.example.domain.models.SimpleWhiteBoardEvent
import org.example.services.InMemoryService
import org.example.services.TransformService
import java.util.concurrent.atomic.AtomicBoolean

class InMemoryWorker(
    private val partitionScope: CoroutineScope,
    private val partition: TopicPartition,
    private val inMemoryService: InMemoryService,
    private val transformService: TransformService,
    private val redisPublisher: RedisPublisher,
    private val flushBatch: (List<ConsumerRecord<String, String>>) -> Unit
) {
    private val running = AtomicBoolean(false)
    private var timerJob: Job? = null
    private var batch = emptyList<ConsumerRecord<String, String>>()

    /**
     * Starts the worker's background processing loop.
     * This loop includes a periodic timer to flush incomplete batches.
     */
    fun start() {
        if (running.compareAndSet(false, true)) {
            println("💾 Starting DB Persistence Worker for partition ${partition.partition()}")
            timerJob = partitionScope.launch {
                while (isActive) { // Loop until the scope is cancelled.
                    // Wait for 1 minute.
                    delay(10_000L)

                    // After the delay, flush any pending records from the current batch.
                    println("⏰ DB Worker for partition [${partition.partition()}] timer fired. Flushing batch...")
                    flushBatch(batch)
                    clearBatch()
                }
            }
        }
    }

    suspend fun proceedRecords(records: List<ConsumerRecord<String, String>>): List<ConsumerRecord<String, String>> {
        println("[In-Memory Worker] - proceed records")
        batch = batch.plus(records)
        val objectMapper = jacksonObjectMapper()
        val events = records.map { it -> objectMapper.readValue<SimpleWhiteBoardEvent>(it.value()) }
        transformService.transform(events)
        inMemoryService.updateWhiteboardState(records.first().key(), records.first().value())
        redisPublisher.mockPublish()
        return if (batch.size == 5) {
            batch.also {
                clearBatch()
            }
        } else {
            emptyList()
        }
    }

    private fun clearBatch() {
        batch = emptyList()
    }

    fun shutdown() {
    }

}