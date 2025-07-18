package org.example.services

import kotlinx.coroutines.delay

// --- Simulated Notification Service ---
// This object simulates sending alerts to engineers (e.g., Slack, Telegram).
object NotificationService {
    suspend fun sendAlert(message: String) {
        println("[ALERT] Sending notification: $message")
        // In a real application, integrate with Slack API, Telegram Bot API, PagerDuty, etc.
        delay(50) // Simulate notification sending latency
    }
}
