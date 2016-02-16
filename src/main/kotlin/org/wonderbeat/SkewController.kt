package org.wonderbeat

import com.google.common.util.concurrent.RateLimiter
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Kinda Monotonic checker
 */
class SkewController(val maxSkew: Int, val bucketsIds: List<Int>) {

    private val logger = LoggerFactory.getLogger(SkewController::class.java)

    private val rateLimiter = RateLimiter.create(0.05)

    private val buckets = (1..bucketsIds.sortedDescending().first()).map { AtomicLong(0) }

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
            logSkewState(minMax.first)
        }
        return SkewState(skew, state, minMax.first, minMax.second)
    }

    private fun logSkewState(minBucketVal: Long) {
        val statStrings = printVertically(buckets.mapIndexed { i, value ->
            val skewSize = (value.get() - minBucketVal).toInt()
            val normalized = if(skewSize == 0) 0 else (((maxSkew.toDouble() + 1) / skewSize ) * 10).toInt()
            "${bucketsIds[i]}".padEnd(3, ' ').padEnd(normalized, 'x')
        }, 100)
        logger.debug("Skew detected\n$statStrings")
    }
}
