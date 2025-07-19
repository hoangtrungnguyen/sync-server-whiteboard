package org.example.services

import kotlinx.coroutines.delay
import org.example.data.LastCommittedOffset
import org.example.data.PersistedOperation
import org.example.data.PersistedWhiteboardSnapshot
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random


// --- Simulated Persistence Database (for saving operations and snapshots) ---
object DbPersistenceService {
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

        println("💽💾 ✅✅✅ [Persistence DB] Successfully saved batch of ${ops.size} operations.")
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