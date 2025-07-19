package org.example.services

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.example.domain.models.SimpleWhiteBoardEvent
import org.example.domain.models.WhiteBoard
import java.util.concurrent.ConcurrentHashMap


class InMemoryService(
) {
    private val whiteboardStates = ConcurrentHashMap<String, WhiteBoard>()

    fun getAllWhiteboards(): List<WhiteBoard> {
        return whiteboardStates.values.toList()
    }

    fun applyOperation(whiteboardId: Int, message: String) {
        println("[In-Memory] Applying operation $whiteboardId")

    }

    fun updateWhiteboardState(whiteboardId: String, newState: String) {
        println("[IN-MEMORY SERVICE] - update white board")
        val objectMapper = jacksonObjectMapper()
        val whiteboardEvent = objectMapper.readValue<SimpleWhiteBoardEvent>(newState)
        println("[IN-MEMORY SERVICE] - whiteboardId:${whiteboardId} - processing ${whiteboardEvent}")
        whiteboardStates.putIfAbsent(
            whiteboardId, WhiteBoard(
                whiteboardId,
                version = 1
            )
        )

        val currentWhiteboard = whiteboardStates[whiteboardId]!!
        whiteboardStates[whiteboardId] = currentWhiteboard.copy(
            version = currentWhiteboard.version + 1
        )
    }

    fun getWhiteboardState(id: String): WhiteBoard? {
        val data = whiteboardStates[id]
        return data
    }

}