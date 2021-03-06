package org.wonderbeat

import com.google.common.util.concurrent.RateLimiter
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

interface SkewControl {
    fun tryAdvance(id: Int): Boolean
}

object NoopSkewControl: SkewControl {
    override fun tryAdvance(id: Int): Boolean = true
}

/**
 * Kinda Monotonic checker
 */
class ConcurrentSkewControl(private val maxSkew: Int,
                            private val bucketsIds: List<Int>,
                            private val limitLoggingPermitsPerSecond: RateLimiter = RateLimiter.create(0.05)): SkewControl {

    private val logger = LoggerFactory.getLogger(ConcurrentSkewControl::class.java)

    private val buckets = (1..bucketsIds.size).map { AtomicLong(0) }

    override fun tryAdvance(id: Int) = tryAdvance(id, 1)

    fun tryAdvance(bucketId: Int, size: Long): Boolean {
        val bucketPosition = bucketsIds.indexOf(bucketId)
        val bucket = buckets[bucketPosition]
        do {
            val bucketVal = bucket.get()
            val skewState = isSkewedWithState()
            if(skewState.isSkewed && bucketVal == skewState.max) {
                return false
            }
        } while(skewState.bucketState[bucketPosition] != bucketVal || !bucket.compareAndSet(bucketVal, bucketVal + size))
        return true
    }

    fun isSkewed(): Boolean = isSkewedWithState().isSkewed

    private data class SkewState(val isSkewed: Boolean, val bucketState: List<Long>, val min: Long, val max: Long)

    private fun isSkewedWithState(): SkewState {
        val state = buckets.map { it.get() }
        val minMax = state.fold(Pair(state.first(), state.first()), { acc, item ->
            Pair(if (acc.first < item) acc.first else item, if(acc.second > item) acc.second else item )
        })
        val skew = minMax.second - minMax.first > maxSkew
        if(skew && logger.isDebugEnabled && limitLoggingPermitsPerSecond.tryAcquire()) {
            val slowestPartitionNum = bucketsIds[state.indexOf(minMax.first)]
            val fastestPartitionNum = bucketsIds[state.indexOf(minMax.second)]
            logger.debug("Skew detected. $slowestPartitionNum slower than $fastestPartitionNum ")
        }
        return SkewState(skew, state, minMax.first, minMax.second)
    }

}
