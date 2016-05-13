package org.wonderbeat

import kafka.message.CompressionCodec
import kafka.producer.SyncProducer
import kafka.producer.SyncProducerConfig
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.ObjectPool
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.util.*

class PartitionConnectionPool<T>(val connections: ConnectionsPool<T>,
                                 val partitionToHostLeader: Map<Int, HostPort>) {
    fun borrowConnection(partition: Int): T? = connections.hostToConnection[partitionToHostLeader[partition]]?.borrowObject()
    fun returnConnection(partition: Int, con: T) = connections.hostToConnection[partitionToHostLeader[partition]]!!.returnObject(con)
}

class ConnectionsPool<T>(hostList: Set<HostPort>,
                         private val constructor: (hostPort: HostPort) -> T,
                         private val destructor: (T) -> Unit,
                         private val poolCfg: GenericObjectPoolConfig = ConnectionsPool.defaultPoolCfg()) {

    companion object {

        fun genericPool(maxTotal: Int, maxIdle: Int = maxTotal, minIdle: Int = maxTotal / 2): GenericObjectPoolConfig {
            val poolCfg = GenericObjectPoolConfig()
            poolCfg.maxTotal = maxTotal
            poolCfg.maxIdle = maxIdle
            poolCfg.minIdle = minIdle
            return poolCfg
        }

        /**
         * Wrapper around dirty Kafka API
         */
        fun syncProducer(hostPort: HostPort, socketTimeoutMills: Int, requestTimeout: Int, compressionCodec: CompressionCodec,
                         sendBufferBytes: Int = 3*1024*1024, clientId: String = "aquana-producer"): SyncProducer {
            val p = Properties()
            p.put("host", hostPort.host)
            p.put("port", hostPort.port.toString())
            p.put("socket.timeout.ms", socketTimeoutMills)
            p.put("request.timeout.ms", requestTimeout.toString())
            p.put("send.buffer.bytes", sendBufferBytes.toString() )
            p.put("client.id",  clientId)
            p.put("compression.codec", compressionCodec.name())
            return SyncProducer(SyncProducerConfig(p))
        }

        fun defaultPoolCfg(): GenericObjectPoolConfig {
            val poolCfg = GenericObjectPoolConfig()
            poolCfg.maxIdle = 5
            poolCfg.maxTotal = 6
            poolCfg.minIdle = 2
            return poolCfg
        }
    }

    private fun internalFactory(host: HostPort) = object: BasePooledObjectFactory<T>() {
        override fun create(): T = constructor(host)
        override fun destroyObject(p: PooledObject<T>) = destructor(p.`object`)
        override fun wrap(obj: T): PooledObject<T>? = DefaultPooledObject<T>(obj)
    }

    fun close() = hostToConnection.forEach { it.value.close() }

    val hostToConnection: Map<HostPort, ObjectPool<T>> = hostList.associateBy({it},
            { GenericObjectPool<T>(internalFactory(it), poolCfg)})

}
