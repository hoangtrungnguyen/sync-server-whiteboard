package org.example

import org.example.services.InMemoryWhiteboardCache
import org.example.services.PersistenceDatabase
import org.example.workers.DbPersistenceWorker
import org.example.workers.InMemoryWorker
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*
import kotlin.system.exitProcess

class WhiteboardApp {

    // Instantiate our two worker components
    private val inMemoryWorker = InMemoryWorker()
    private val dbPersistenceWorker = DbPersistenceWorker()
    private lateinit var appScope: CoroutineScope // Main scope for managing application-level coroutines

    // Main application entry point. `runBlocking` is used here to keep the JVM alive
    // while coroutines are running, suitable for a main application loop.
    fun start(): Nothing = runBlocking {
        // Create a parent CoroutineScope for all application-level coroutines.
        // Using `SupervisorJob()` ensures that if a child coroutine fails,
        // it doesn't automatically cancel other sibling coroutines.
        appScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

        println("--- Whiteboard Application Starting ---")

        // --- Application Initialization Sequence ---

        // 1. Load metadata (document snapshots) from persistence database
        println("\n[INIT] Step 1: Loading existing whiteboard snapshots from persistence database...")
        val initialSnapshots = PersistenceDatabase.loadAllSnapshots()
        initialSnapshots.forEach { snapshot ->
            InMemoryWhiteboardCache.loadSnapshot(snapshot) // Load each snapshot into the in-memory cache
        }
        println("[INIT] Finished loading initial snapshots. ${InMemoryWhiteboardCache.whiteboards.size} whiteboards initialized in memory.")

        // 2. Launch In-Memory Worker: It will process messages from its last committed offset (or earliest if new)
        // Its Kafka consumer will automatically handle catching up to the current state based on its group's offsets.
        val inMemoryJob = inMemoryWorker.start(appScope)

        // 3. Launch DB Persistence Worker: It will process messages from its last committed offset.
        // It's also responsible for periodically saving snapshots (the "newest document" idea)
        // and its own offset to the persistence database after successful batch writes.
        // The snapshotting aspect is integrated into the worker itself, or a dedicated periodic job.
        // For a more direct "save newest document to DB" on startup *after* in-memory catchup,
        // you would need a mechanism for `inMemoryWorker` to signal "caught up", then trigger
        // a `PersistenceDatabase.saveWhiteboardSnapshot` here, possibly taking data from `InMemoryWhiteboardCache`.
        // Our current solution relies on a periodic snapshotting inside the persistence worker.
        val dbPersistenceJob = dbPersistenceWorker.start(appScope)

        // 4. (Implicitly handled) DB persistence worker saving newly offset after in-memory processes un-marked messages.
        // In our chosen Solution C, each worker manages its *own* offsets independently within its consumer group.
        // The DB Persistence Worker saves *its own* last processed offset to the persistence DB after *its own* successful batch writes.
        // This decouples their offset management, making the system more robust and easier to reason about.

        // 5. (Implicitly handled) App publish transformed operation to REDIS database.
        // This is performed by the In-Memory Worker as part of its processing loop.


        println("\n--- Whiteboard Application Running ---")
        println("Application initialized and workers are running. Press Enter to stop...")
        readlnOrNull() // Keep the main coroutine (and thus the app) alive until user input

        println("\n--- Whiteboard Application Stopping ---")
        // Signal workers to stop gracefully
        inMemoryWorker.stop()
        dbPersistenceWorker.stop()

        // Wait for worker coroutines to complete their shutdown procedures
        // `join()` suspends until the coroutine's job is complete.
        inMemoryJob.join()
        dbPersistenceJob.join()

        // Cancel the main application scope, which will cancel any remaining child coroutines.
        appScope.cancel()
        println("All workers stopped. Application shut down gracefully.")

        exitProcess(0) // Exit the JVM
    }
}

// Main function to start the application
fun main() {
    WhiteboardApp().start()
}