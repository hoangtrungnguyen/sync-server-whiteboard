package org.example.data.redis

import kotlinx.coroutines.delay
import org.example.data.WhiteboardOperationMessage
import kotlin.random.Random

// --- Simulated Redis Publisher ---
// This object simulates publishing transformed operations to a Redis database for real-time propagation.
object RedisPublisher {
    suspend fun publishTransformedOperation(operation: WhiteboardOperationMessage) {
        // In a real application, you would use a Redis client (e.g., Lettuce, Jedis)
        // to PUBLISH to a Redis channel or store in a list/stream.
        println("[Redis] Publishing transformed operation '${operation.operationId}' for whiteboard '${operation.whiteboardId}'")
        delay(Random.nextLong(10, 50)) // Simulate Redis network latency
    }

    suspend fun mockPublish() {
        println("🌬️🔥 ✅✅✅ [Redis] Publishing transformed operation")
        delay(Random.nextLong(10, 50))
    }
}
