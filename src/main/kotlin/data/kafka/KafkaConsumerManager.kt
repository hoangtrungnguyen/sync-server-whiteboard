package org.example.data.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.example.config.KAFKA_BOOTSTRAP_SERVERS
import org.example.config.KAFKA_GROUP_ID
import org.example.config.KAFKA_TOPICS
import org.example.workers.KafkaPartitionProcessor
import java.time.Duration
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class KafkaConsumerManager(
    private val processor: KafkaPartitionProcessor
) {

    private val consumer: KafkaConsumer<String, String>
    private val running = AtomicBoolean(false)
    private val consumerThread: ExecutorService = Executors.newSingleThreadExecutor()

    init {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS)
            put(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP_ID)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
        }
        consumer = KafkaConsumer(props)
    }

    fun start() {
        if (running.compareAndSet(false, true)) {
            consumerThread.submit { pollLoop() }
        }
    }

    private fun pollLoop() {
        try {
            consumer.subscribe(KAFKA_TOPICS) // <-- CHANGE TO YOUR TOPIC
            println("Consumer subscribed and polling loop started.")
            while (running.get()) {
                val records = consumer.poll(Duration.ofMillis(500))
                if (!records.isEmpty) {
                    println("Polled ${records.count()} records.")
                    records.partitions().forEach { partition ->
                        val partitionRecords = records.records(partition)
                        processor.submit(partition, partitionRecords)
                    }
                }
            }
        } catch (e: Exception) {
            println("ERROR in poll loop: ${e.message}")
        } finally {
            println("Closing Kafka consumer.")
            consumer.close()
        }
    }

    fun shutdown() {
        running.set(false)
        consumer.wakeup()
        consumerThread.shutdown()
        try {
            if (!consumerThread.awaitTermination(10, TimeUnit.SECONDS)) {
                consumerThread.shutdownNow()
            }
        } catch (e: InterruptedException) {
            consumerThread.shutdownNow()
        }
    }

}
