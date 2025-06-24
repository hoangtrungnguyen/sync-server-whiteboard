// src/main/kotlin/com/example/whiteboard/data/WhiteboardOperations.kt
package org.example.data

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.StampedLock // More flexible than ReadWriteLock

// --- Kafka Message Data Class (JSON structure) ---
// This represents the structure of the JSON messages consumed from Kafka.
@JsonIgnoreProperties(ignoreUnknown = true) // Gracefully ignore any fields not mapped here
data class WhiteboardOperationMessage(
    val whiteboardId: String,          // Identifier for the specific whiteboard
    val sessionId: String,             // Identifier for the user/client session
    val operationId: String,           // Unique ID for this specific operation
    val timestamp: Instant,            // Timestamp of the operation
    val operationType: OperationType,  // Type of whiteboard operation (e.g., DRAWING, ERASE)
    val payload: Map<String, Any?>     // Dynamic map for the "big JSON" operation data
)

// Enum to categorize different types of whiteboard operations
enum class OperationType {
    DRAWING, ERASE, ADD_SHAPE, MOVE_ELEMENT, OTHER
}

// --- In-Memory Hybrid Data Structure for Whiteboard State ---
// This represents the current, real-time state of a single whiteboard in memory.
// It reflects the hybrid table+tree structure you described.
data class WhiteboardState(
    val whiteboardId: String,
    @Volatile var title: String = "Untitled Whiteboard", // Mutable field, volatile for thread visibility
    @Volatile var status: String = "ACTIVE",          // Mutable field, volatile for thread visibility
    @Volatile var rootComponent: WhiteboardComponent, // The root of the tree structure for components/groups
    val elements: ConcurrentHashMap<String, ElementData>, // A concurrent map for fast O(1) lookup of individual elements by their ID
    @Transient val lock: StampedLock = StampedLock() // Per-whiteboard lock for concurrent read/write access
) {
    // Secondary constructor to easily initialize a new, empty whiteboard state
    constructor(whiteboardId: String) : this(
        whiteboardId = whiteboardId,
        rootComponent = WhiteboardComponent( // Initialize with a default root component
            id = UUID.randomUUID().toString(),
            type = "WHITEBOARD_ROOT",
            content = "Root for $whiteboardId"
        ),
        elements = ConcurrentHashMap() // Initialize an empty map for elements
    )
}

// Data class representing an individual element on the whiteboard (part of the "Element Table")
data class ElementData(
    val elementId: String,
    @Volatile var content: String, // The actual content of the element (e.g., SVG path, text)
    @Volatile var parentId: String?, // Parent element's ID in the hierarchy (nullable for top-level)
    @Volatile var path: String?,     // Full hierarchical path (e.g., "/root/section/drawing1")
    @Volatile var isDeleted: Boolean = false // Flag for soft deletion
)

// Data class representing a node in the 'rootComponent' tree structure (for components/groups)
data class WhiteboardComponent(
    val id: String,
    @Volatile var content: String?, // Content for this component (e.g., section title)
    val type: String,               // Type of component (e.g., "SECTION", "GROUP", "CONTAINER")
    val children: MutableList<WhiteboardComponent> = mutableListOf(), // Nested children for the tree structure
    @Volatile var elementRefId: String? = null // Optional reference to an ElementData ID if this component represents a concrete element
)

// --- Persistence Metadata for DB Persistence Worker ---
// Stores the last committed offset for a consumer group and topic-partition in the persistence DB.
data class LastCommittedOffset(
    val consumerGroupId: String,
    val topicName: String,
    val partition: Int,
    val offset: Long
)

// --- Utility for JSON Serialization/Deserialization (Jackson Object Mapper) ---
// Configured to support Kotlin data classes and Java 8 Date/Time types.
val objectMapper = ObjectMapper().apply {
    registerModule(KotlinModule.Builder().build()) // Register Kotlin module for seamless data class mapping
    findAndRegisterModules() // Auto-discover and register other modules (like JSR310 for Instant)
}

// --- Simplified Database Models for Persistence Worker's DB ---
// Represents a row in the "operations log" table in the persistence database.
data class PersistedOperation(
    val id: Long = 0, // Auto-incremented ID in the DB
    val whiteboardId: String,
    val sessionId: String,
    val operationId: String,
    val timestamp: Instant,
    val operationType: String, // Store enum name as string
    val payloadJson: String, // Store the entire operation payload as a raw JSON string
    // Kafka metadata for auditing and recovery
    val kafkaTopic: String,
    val kafkaPartition: Int,
    val kafkaOffset: Long
)

// Represents a full snapshot of a whiteboard's state, saved periodically/on events.
data class PersistedWhiteboardSnapshot(
    val whiteboardId: String,
    val title: String,
    val status: String,
    val rootComponentJson: String, // Store the root component tree as a JSON string
    val elementsJson: String, // Store the elements map as a JSON string
    val lastKafkaOffset: Long, // The highest Kafka offset processed to build this snapshot
    val snapshotTimestamp: Instant // When this snapshot was taken
)
