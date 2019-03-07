package com.orientdb.improt

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ImprotApplication

fun main(args: Array<String>) {
    val app = SpringApplication(ImprotApplication::class.java)
    app.addListeners(ImportListener())
    app.run(*args)
}
