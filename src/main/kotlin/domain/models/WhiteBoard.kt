package org.example.domain.models

data class WhiteBoard(
    val title: String,
//    val children:
    val version: Int
)


data class SimpleWhiteBoardEvent(
    val id: Long,
    val user: String, val event: String

)