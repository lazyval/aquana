package org.wonderbeat

import kafka.consumer.SimpleConsumer

data class PartitionMeta(val topic: String, val partition: Int,
                         val leader: HostPort, val startOffset: Long, val endOffset: Long)

fun getPartitionsMeta(consumersPool: ConnectionsPool<SimpleConsumer>, leaders: Map<Int, HostPort>, topic: String):
        List<PartitionMeta> {
    fun <T> tryResolve(resolve: (consumer: SimpleConsumer) -> T, host: HostPort): T {
        val connectionPool = consumersPool.hostToConnection[host]!!
        val consumer = connectionPool.borrowObject()!!
        try {
            return resolve(consumer)
        } finally {
            connectionPool.returnObject(consumer)
        }
    }
    val out = leaders.map { entry ->
        val beginPosition = tryResolve({ it.resolveOffsets(topic, listOf(entry.key), Position.BEGIN) }, entry.value)
        val endPosition = tryResolve({ it.resolveOffsets(topic, listOf(entry.key), Position.END) }, entry.value)
        PartitionMeta(topic, entry.key, entry.value,
            beginPosition.entries.first().value,
            endPosition.entries.first().value)
    }
    return out

}

