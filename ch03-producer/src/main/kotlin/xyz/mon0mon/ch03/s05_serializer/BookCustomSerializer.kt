package xyz.mon0mon.ch03.s05_serializer

import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serializer
import java.nio.ByteBuffer

class BookCustomSerializer : Serializer<Customer> {
    /**
     * We are serializing Customer as:
     * 4 byte int representing customerId
     * 4 byte int representing length of customerName in UTF-8 bytes (0 if name is Null)
     * N bytes representing customerName in UTF-8
     */
    override fun serialize(topic: String?, data: Customer?): ByteArray? {
        try {
            if (data == null) return null

            val serializedName = if (data.customerName.isNotBlank()) data.customerName.toByteArray(Charsets.UTF_8) else ByteArray(0)
            val stringSize = if (data.customerName.isNotBlank()) data.customerName.length else 0

            val buffer = ByteBuffer.allocate(4 + 4 + stringSize)
            buffer.putInt(data.customerId)
            buffer.putInt(stringSize)
            buffer.put(serializedName)

            return buffer.array()
        } catch (e: Exception) {
            throw SerializationException("Error when serializing Customer to byte[] $e")
        }
    }
}
