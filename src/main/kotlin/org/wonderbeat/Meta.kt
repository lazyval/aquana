package org.wonderbeat

import kafka.consumer.SimpleConsumer

data class PartitionMeta(val topic: String, val partition: Int,
                         val leader: String, val startOffset: Long, val endOffset: Long)

fun getPartitionsMeta(consumersPool: ConnectionsPool<SimpleConsumer>, leaders: Map<Int, String>, topic: String):
        List<PartitionMeta> {
    fun tryResolve<T>(funct: (consumer: SimpleConsumer) -> T, host: String): T {
        val connectionPool = consumersPool.hostToConnection[host]!!
        val consumer = connectionPool.borrowObject()!!
        try {
            return funct(consumer)
        } finally {
            connectionPool.returnObject(consumer)
        }
    }
    val out = leaders.map { entry -> PartitionMeta(topic, entry.key, entry.value,
            tryResolve({ it.resolveOffsets(topic, listOf(entry.key), Position.BEGIN)}, entry.value)
                    .entries.first().value,
            tryResolve({ it.resolveOffsets(topic, listOf(entry.key), Position.END)}, entry.value)
                    .entries.first().value)  }
    return out

}

