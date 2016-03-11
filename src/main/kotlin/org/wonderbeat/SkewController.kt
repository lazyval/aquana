package org.wonderbeat

import com.google.common.util.concurrent.RateLimiter
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Kinda Monotonic checker
 */
class SkewController(val maxSkew: Int, val bucketsIds: List<Int>, val limitLoggingPermitsPerSecond: Double = 0.05) {

    private val logger = LoggerFactory.getLogger(SkewController::class.java)

    private val rateLimiter = RateLimiter.create(limitLoggingPermitsPerSecond)

    private val buckets = (1..bucketsIds.size).map { AtomicLong(0) }

    fun tryAdvance(bucketId: Int): Boolean {
        val bucketPosition = bucketsIds.indexOf(bucketId)
        val bucket = buckets[bucketPosition]
        do {
            val bucketVal = bucket.get()
            val skewState = isSkewedWithState()
            if(skewState.isSkewed && bucketVal == skewState.max) {
                return false
            }
        } while(skewState.bucketState[bucketPosition] != bucketVal || !bucket.compareAndSet(bucketVal, bucketVal + 1))
        return true
    }

    fun isSkewed(): Boolean {
        return isSkewedWithState().isSkewed
    }

    private data class SkewState(val isSkewed: Boolean, val bucketState: List<Long>, val min: Long, val max: Long)

    private fun isSkewedWithState(): SkewState {
        val state = buckets.map { it.get() }
        val minMax = state.fold(Pair(state.first(), state.first()), { acc, item ->
            Pair(if (acc.first < item) acc.first else item, if(acc.second > item) acc.second else item )
        })
        val skew = minMax.second - minMax.first > maxSkew
        if(skew && logger.isDebugEnabled && rateLimiter.tryAcquire()) {
            val slowestPartitionNum = bucketsIds[state.indexOf(minMax.first)]
            val fastestPartitionNum = bucketsIds[state.indexOf(minMax.second)]
            logger.debug("Skew detected. $slowestPartitionNum slower than $fastestPartitionNum ")
        }
        return SkewState(skew, state, minMax.first, minMax.second)
    }

}
