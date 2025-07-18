package org.example

import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import org.example.data.kafka.KafkaConsumerManager
import org.example.services.DbPersistenceService
import org.example.services.InMemoryService
import org.example.services.TransformService
import org.example.workers.KafkaPartitionProcessor


fun Application.module() {
    println("Setting up CQRS application module...")


    val dbService = DbPersistenceService
    val transformService = TransformService()
    val inMemoryService = InMemoryService

    val kafkaPartitionProcessor = KafkaPartitionProcessor(
        inMemoryService,
        dbService,
        transformService,
    )

    val kafkaConsumerManager = KafkaConsumerManager(
        kafkaPartitionProcessor,
    )

    routing {
        get("/") {
            call.respondText("Hello, Ktor!")
        }

        get("/whiteboards") {
            val whiteboards = inMemoryService.getAllWhiteboards()
            call.respondText(whiteboards.toString())
        }

        get("/whiteboards/{id}") {
            val id = call.parameters["id"]
            val state = inMemoryService.getWhiteboardState(id ?: "")
            if (state != null) {
                call.respondText("State for whiteboard '$id': $state")
            } else {
                call.respondText("No state found for whiteboard '$id'")
            }
        }
    }

    environment.monitor.subscribe(ApplicationStarted) {
        log.info("Application started. Starting Kafka consumer manager...")
        kafkaConsumerManager.start()
    }

    environment.monitor.subscribe(ApplicationStopping) {
        log.info("Application stopping. Shutting down Kafka consumer and processor...")
        kafkaConsumerManager.shutdown()
        kafkaPartitionProcessor.shutdown()
        log.info("Shutdown complete.")
    }

}

// Main function to start the application
fun main() {
    embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
        module()
    }.start(wait = true)
}