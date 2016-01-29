package org.wonderbeat

import com.google.common.util.concurrent.RateLimiter
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong

/**
 * Kinda Monotonic checker
 */
class SkewController(val maxSkew: Int, val bucketsSize: Int) {

    private val logger = LoggerFactory.getLogger(SkewController::class.java)

    private val rateLimiter = RateLimiter.create(0.05)

    private val buckets = bucketsSize.downTo(1).map { AtomicLong(0) }

    fun tryAdvance(bucket: Int): Boolean {
        do {
            val bucketVal = buckets[bucket].get()
            val (isSkewed, state) = isSkewedWithState()
            if(isSkewed) {
                return false
            }
        } while(state[bucket] != bucketVal || !buckets[bucket].compareAndSet(bucketVal, bucketVal + 1))
        return true
    }

    fun isSkewed(): Boolean {
        return isSkewedWithState().first
    }

    private fun isSkewedWithState(): Pair<Boolean, List<Long>> {
        val state = buckets.map { it.get() }
        val minMax = state.fold(Pair(state.first(), state.first()), { acc, item ->
            Pair(if (acc.first < item) acc.first else item, if(acc.second > item) acc.second else item )
        })
        val skew = minMax.second - minMax.first > maxSkew
        if(skew && logger.isDebugEnabled && rateLimiter.tryAcquire()) {
            logSkewState(minMax.first)
        }
        return Pair(skew, state)
    }

    private fun logSkewState(minBucketVal: Long) {
        val statStrings = printVertically(buckets.mapIndexed { i, value ->
            val skewSize = (value.get() - minBucketVal).toInt()
            val normalized = if(skewSize == 0) 0 else (((maxSkew.toDouble() + 1) / skewSize ) * 10).toInt()
            "$i".padEnd(3, ' ').padEnd(normalized, 'x')
        }, 100)
        logger.debug("Skew detected\n$statStrings")
    }
}
