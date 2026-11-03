package xyz.mon0mon.ch03.s05_serializer

import customerManagement.avro.Customer

object CustomerGenerator {
    private var nextId = 0

    fun getNext(): Customer {
        val id = nextId++
        return Customer(
            id = id,
            name = "exampleCustomer$id",
            email = "example$id@example.com",
        )
    }
}
