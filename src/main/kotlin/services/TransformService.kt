package org.example.services

class TransformService {

    fun transform() {
        Thread.sleep(50) // Simulate CPU-bound work
        println("executed TRANSFORM")
    }
}