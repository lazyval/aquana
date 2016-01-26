package org.wonderbeat

import kafka.api.OffsetRequest
import kafka.api.PartitionOffsetRequestInfo
import kafka.api.Request
import kafka.api.TopicMetadataRequest
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import scala.collection.JavaConversions.*
import scala.collection.Seq


fun SimpleConsumer.resolveLeaders(topic: String): Map<Int, String> {
    val request = TopicMetadataRequest(kafka.api.TopicMetadataRequest.CurrentVersion(), 0, kafka.api
            .TopicMetadataRequest.DefaultClientId(), asScalaBuffer(listOf(topic)))
    return asJavaList(asJavaList(this.send(request).topicsMetadata()).get(0).partitionsMetadata()).toMap({it
            .partitionId()}, {it.leader().get().host()})
}

enum class Position { BEGIN, END }

fun SimpleConsumer.resolveOffsets(topic: String, partitions: List<Int>, position: Position): Map<Int, Long> {
    val position = if (position == Position.BEGIN) kafka.api.OffsetRequest.EarliestTime() else kafka.api.OffsetRequest.LatestTime()
    val request = partitions.toMap({ TopicAndPartition(topic, it) }, { PartitionOffsetRequestInfo(position, 1) })
    val response = this.getOffsetsBefore(OffsetRequest(
            ToScalaMap.toScalaMap(request), 0, Request
            .OrdinaryConsumerId()))
    return asJavaMap(response.partitionErrorAndOffsets())
            .mapKeys { it.key.partition() }
            .mapValues { asJavaList<Long>(it.value.offsets() as Seq<Long>).first() }
}
