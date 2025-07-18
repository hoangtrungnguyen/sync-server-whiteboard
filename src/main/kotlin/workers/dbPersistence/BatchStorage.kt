package org.example.workers.dbPersistence

import java.util.*


class BatchStorage<T> {
    private val batchSize = 10
    private val currentBatch = mutableListOf<T>()
    private val readyBatches: Queue<MutableList<T>> = LinkedList()

    /**
     * Adds an item to the current batch. If the batch becomes full,
     * it's added to the queue of ready batches.
     */
    fun add(item: T) {
        currentBatch.add(item)
        if (currentBatch.size >= batchSize) {
            readyBatches.add(ArrayList(currentBatch)) // Add a *copy*
            currentBatch.clear()
        }
    }

    /**
     * Forces any items remaining in the current batch to be queued for processing.
     * This is crucial for handling records that don't perfectly fill a batch.
     */
    fun flush() {
        if (currentBatch.isNotEmpty()) {
            readyBatches.add(ArrayList(currentBatch)) // Add a *copy*
            currentBatch.clear()
        }
    }

    /**
     * Retrieves the next complete batch from the queue.
     * @throws NoSuchElementException if the queue is empty.
     */
    fun getBatch(): List<T> {
        if (readyBatches.isEmpty()) {
            throw NoSuchElementException("No batches are ready in the queue.")
        }
        return readyBatches.remove()
    }

    /**
     * Checks if there is at least one complete batch ready for processing.
     */
    fun isBatchReady(): Boolean {
        return readyBatches.isNotEmpty()
    }

}