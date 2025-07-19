package org.example.services

import org.example.domain.models.SimpleWhiteBoardEvent

class TransformService {

    fun transform(events: List<SimpleWhiteBoardEvent>): SimpleWhiteBoardEvent {
        Thread.sleep(50) // Simulate CPU-bound work
        println("executed TRANSFORM")

        return SimpleWhiteBoardEvent(
            id = System.nanoTime(),
            user = events.first().user,
            event = events.first().event + " transformed"
        )
    }
}