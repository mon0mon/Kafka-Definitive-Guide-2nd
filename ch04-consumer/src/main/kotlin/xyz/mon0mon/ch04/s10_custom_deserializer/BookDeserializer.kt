package xyz.mon0mon.ch04.s10_custom_deserializer

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Deserializer
import java.nio.ByteBuffer

class BookDeserializer : Deserializer<Customer> {
    override fun deserialize(
        topic: String?, data: ByteArray?
    ): Customer? {
        try {
            data ?: return null

            if (data.size < 8)
                throw SerializationException("Size of dat received by deserializer is shorter than expected")

            val buffer = ByteBuffer.wrap(data)
            val id = buffer.getInt()
            val nameSize = buffer.getInt()

            val nameBytes = ByteArray(nameSize)
            buffer.get(nameBytes)

            val name = String(nameBytes, Charsets.UTF_8)

            return Customer(id, name)
        } catch (e: Exception) {
            throw SerializationException("Error when deserializing byte[] to Customer", e.cause)
        }
    }
}
