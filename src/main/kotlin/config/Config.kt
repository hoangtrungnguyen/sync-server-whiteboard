package org.example.config


const val KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
val KAFKA_TOPICS = listOf("whiteboard-events")
const val KAFKA_GROUP_ID = "whiteboard-processor-group-cqrs"