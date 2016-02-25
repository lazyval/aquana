package org.wonderbeat

import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.api.Request
import kafka.api.TopicMetadataRequest
import kafka.common.ErrorMapping
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import scala.collection.JavaConversions.*
import scala.collection.Seq


data class HostPort(val host: String, val port: Int)

fun SimpleConsumer.resolveLeaders(topic: String): Map<Int, HostPort> {
    val request = TopicMetadataRequest(kafka.api.TopicMetadataRequest.CurrentVersion(), 0, kafka.api
            .TopicMetadataRequest.DefaultClientId(), asScalaBuffer(listOf(topic)))
    val response = this.send(request)
    return asJavaList(asJavaList(response.topicsMetadata())[0].partitionsMetadata()).associateBy({it
            .partitionId()}, {
        val leader = it.leader().get()
        HostPort(leader.host(), leader.port())})
}

enum class Position { BEGIN, END }

fun SimpleConsumer.resolveOffsets(topic: String, partitions: List<Int>, position: Position): Map<Int, Long> {
    val positionMarker = if (position == Position.BEGIN) kafka.api.OffsetRequest.EarliestTime() else kafka.api.OffsetRequest.LatestTime()
    val request = partitions.associateBy({ TopicAndPartition(topic, it) }, { PartitionOffsetRequestInfo(positionMarker, 1) })
    val response = this.getOffsetsBefore(OffsetRequest(ToScalaMap.toScalaMap(request), 0, Request.OrdinaryConsumerId()))
    if(response.hasError()) {
        throw asJavaMap(response.partitionErrorAndOffsets()).map { ErrorMapping.exceptionFor(it.value.error()) }.first()
    }
    return asJavaMap(response.partitionErrorAndOffsets())
            .mapKeys { it.key.partition() }
            .mapValues { asJavaList<Long>(it.value.offsets() as Seq<Long>).first() }
}
