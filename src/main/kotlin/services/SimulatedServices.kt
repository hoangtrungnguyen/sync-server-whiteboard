// src/main/kotlin/com/example/whiteboard/services/SimulatedServices.kt
package org.example.services

import org.example.data.*
import org.example.data.objectMapper
import kotlinx.coroutines.delay
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import com.fasterxml.jackson.module.kotlin.readValue

// --- Simulated In-Memory Whiteboard Cache (The "In-Memory Database") ---
// This object holds the live, transformed state of all whiteboards.
object InMemoryWhiteboardCache {
    // ConcurrentHashMap to store WhiteboardState objects, allowing concurrent access by whiteboardId.
    val whiteboards: ConcurrentHashMap<String, WhiteboardState> = ConcurrentHashMap()

    // Simplified Operational Transformation (OT) application logic.
    // In a real system, this would be a sophisticated algorithm for conflict resolution.
    // Returns true if the operation was applied, false otherwise (e.g., conflict).
    fun applyOperation(whiteboardId: String, operation: WhiteboardOperationMessage): Boolean {
        // Retrieve or create the WhiteboardState for the given whiteboardId.
        val whiteboardState = whiteboards.computeIfAbsent(whiteboardId) { WhiteboardState(whiteboardId) }

        // Acquire a write lock on the specific whiteboard to prevent concurrent modifications
        // while the OT algorithm is being applied. StampedLock is used for flexibility.
        val stamp = whiteboardState.lock.writeLock()
        try {
            println("[In-Memory Cache] Applying OT operation '${operation.operationType}' (${operation.operationId}) to whiteboard '${whiteboardId}'")

            // --- Placeholder for actual OT algorithm logic ---
            // This is where you would implement your complex OT logic to
            // transform the incoming operation based on the current `whiteboardState`
            // and then apply the transformed operation to `whiteboardState.rootComponent`
            // and `whiteboardState.elements`.
            // For example:
            when (operation.operationType) {
                OperationType.DRAWING, OperationType.ADD_SHAPE, OperationType.MOVE_ELEMENT -> {
                    // Assume payload contains necessary fields to update/create an element
                    val elementId = operation.payload["elementId"] as? String ?: operation.operationId // Use operationId as elementId if not provided
                    val content = operation.payload["content"] as? String ?: "Default Content"
                    val parentId = operation.payload["parentId"] as? String
                    val path = operation.payload["path"] as? String

                    whiteboardState.elements.compute(elementId) { _, existing ->
                        if (existing != null) {
                            // Update existing element
                            existing.copy(content = content, parentId = parentId, path = path)
                        } else {
                            // Add new element
                            ElementData(elementId, content, parentId, path)
                        }
                    }
                    // Logic to update `rootComponent` tree to reflect new/modified element's position/hierarchy
                    println("  -> Element '$elementId' updated/added in in-memory cache.")
                }
                OperationType.ERASE -> {
                    val elementId = operation.payload["elementId"] as? String
                    if (elementId != null) {
                        whiteboardState.elements[elementId]?.isDeleted = true // Mark as deleted
                        println("  -> Element '$elementId' marked as deleted in in-memory cache.")
                    }
                }
                else -> {
                    println("  -> Unknown/unhandled operation type: ${operation.operationType}. Skipping detailed processing.")
                }
            }
            // --- End of placeholder for OT logic ---

            return true // Indicate successful application of operation
        } finally {
            whiteboardState.lock.unlockWrite(stamp) // Release the write lock
        }
    }

    // Loads a full whiteboard snapshot into the in-memory cache, typically on application startup.
    fun loadSnapshot(snapshot: PersistedWhiteboardSnapshot) {
        val rootComponent = objectMapper.readValue<WhiteboardComponent>(snapshot.rootComponentJson)
        val elementsMap = objectMapper.readValue<ConcurrentHashMap<String, ElementData>>(snapshot.elementsJson)

        whiteboards[snapshot.whiteboardId] = WhiteboardState(
            whiteboardId = snapshot.whiteboardId,
            title = snapshot.title,
            status = snapshot.status,
            rootComponent = rootComponent,
            elements = elementsMap
        )
        println("[In-Memory Cache] Loaded initial snapshot for whiteboard: ${snapshot.whiteboardId}")
    }

    // Retrieves the current state of a whiteboard from memory. Used for snapshotting.
    fun getWhiteboardState(whiteboardId: String): WhiteboardState? {
        return whiteboards[whiteboardId]
    }
}

// --- Simulated Persistence Database (for saving operations and snapshots) ---
object PersistenceDatabase {
    // In a real application, these would be actual database tables (e.g., PostgreSQL, MySQL).
    // Using ConcurrentHashMaps to simulate tables.
    private val operations = ConcurrentHashMap<Long, PersistedOperation>()
    private val snapshots = ConcurrentHashMap<String, PersistedWhiteboardSnapshot>()
    private val offsets = ConcurrentHashMap<String, LastCommittedOffset>() // Stores offsets for persistence_group
    private val operationIdCounter = AtomicLong(0) // Simple counter for operation ID in simulated DB

    // Simulates saving a batch of operations to the database.
    suspend fun saveOperationsBatch(ops: List<PersistedOperation>): Boolean {
        println("[Persistence DB] Attempting to save batch of ${ops.size} operations...")
        delay(Random.nextLong(50, 200)) // Simulate network/DB write latency

        // Simulate a 5% chance of batch save failure for testing error handling.
        if (Random.nextDouble() < 0.05) {
            println("[Persistence DB] --- SIMULATED BATCH SAVE FAILURE ---")
            return false
        }

        ops.forEach { op ->
            operations[operationIdCounter.incrementAndGet()] = op // Add to simulated table
        }
        println("[Persistence DB] Successfully saved batch of ${ops.size} operations.")
        return true
    }

    // Simulates saving a full whiteboard snapshot.
    suspend fun saveWhiteboardSnapshot(snapshot: PersistedWhiteboardSnapshot): Boolean {
        println("[Persistence DB] Saving whiteboard snapshot for ${snapshot.whiteboardId} at offset ${snapshot.lastKafkaOffset}...")
        delay(Random.nextLong(100, 300)) // Simulate latency
        snapshots[snapshot.whiteboardId] = snapshot // Overwrite with newest snapshot
        println("[Persistence DB] Successfully saved snapshot for ${snapshot.whiteboardId}.")
        return true
    }

    // Loads the last committed offset for a specific consumer group and partition.
    suspend fun loadLastOffset(consumerGroupId: String, topicName: String, partition: Int): LastCommittedOffset? {
        val key = "$consumerGroupId-$topicName-$partition" // Composite key for lookup
        val offset = offsets[key]
        println("[Persistence DB] Loaded last offset for group '$consumerGroupId', partition $partition: ${offset?.offset}")
        return offset
    }

    // Saves the last committed offset for a specific consumer group and partition.
    suspend fun saveLastOffset(offsetInfo: LastCommittedOffset) {
        val key = "${offsetInfo.consumerGroupId}-${offsetInfo.topicName}-${offsetInfo.partition}"
        offsets[key] = offsetInfo
        println("[Persistence DB] Saved last offset for group '${offsetInfo.consumerGroupId}', partition ${offsetInfo.partition}: ${offsetInfo.offset}")
    }

    // Loads all existing whiteboard snapshots from the persistence database.
    suspend fun loadAllSnapshots(): List<PersistedWhiteboardSnapshot> {
        println("[Persistence DB] Loading all whiteboard snapshots...")
        delay(Random.nextLong(200, 500)) // Simulate latency
        return snapshots.values.toList()
    }
}

// --- Simulated Redis Publisher ---
// This object simulates publishing transformed operations to a Redis database for real-time propagation.
object RedisPublisher {
    suspend fun publishTransformedOperation(operation: WhiteboardOperationMessage) {
        // In a real application, you would use a Redis client (e.g., Lettuce, Jedis)
        // to PUBLISH to a Redis channel or store in a list/stream.
        println("[Redis] Publishing transformed operation '${operation.operationId}' for whiteboard '${operation.whiteboardId}'")
        delay(Random.nextLong(10, 50)) // Simulate Redis network latency
    }
}

// --- Simulated Notification Service ---
// This object simulates sending alerts to engineers (e.g., Slack, Telegram).
object NotificationService {
    suspend fun sendAlert(message: String) {
        println("[ALERT] Sending notification: $message")
        // In a real application, integrate with Slack API, Telegram Bot API, PagerDuty, etc.
        delay(50) // Simulate notification sending latency
    }
}
