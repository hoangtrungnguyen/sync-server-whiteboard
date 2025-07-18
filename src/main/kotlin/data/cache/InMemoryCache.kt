package org.example.data.cache

import com.fasterxml.jackson.module.kotlin.readValue
import org.example.data.*
import java.util.concurrent.ConcurrentHashMap

// --- Simulated In-Memory Whiteboard Cache (The "In-Memory Database") ---
// This object holds the live, transformed state of all whiteboards.
object InMemoryCache {
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

