package org.wonderbeat

import com.github.rholder.retry.Attempt
import com.github.rholder.retry.RetryListener
import org.slf4j.LoggerFactory

val logAttemptFailure = object: RetryListener {
    private val logger = LoggerFactory.getLogger("org.wonderbeat.retry")
    override fun <V : Any?> onRetry(attempt: Attempt<V>?) {
        if(attempt?.hasException() == true) {
            logger.warn("Attempt ${attempt?.attemptNumber} failed ${attempt?.exceptionCause} with stacktrace: ${attempt?.exceptionCause?.stackTrace}")
        }
    }
}
