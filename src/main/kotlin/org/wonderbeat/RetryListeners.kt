package org.wonderbeat

import com.github.rholder.retry.Attempt
import com.github.rholder.retry.RetryListener
import org.slf4j.LoggerFactory
import java.io.PrintWriter
import java.io.StringWriter

val logAttemptFailure = object: RetryListener {
    private val logger = LoggerFactory.getLogger("org.wonderbeat.retry")
    override fun <V : Any?> onRetry(attempt: Attempt<V>?) {
        if(attempt?.hasException() == true) {
            val stacktrace = attempt?.exceptionCause?.printStackTrace(PrintWriter(StringWriter()));
            logger.warn("Attempt ${attempt?.attemptNumber} failed ${attempt?.exceptionCause} with stacktrace $stacktrace")
        }
    }
}
