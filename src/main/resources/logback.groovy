import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.core.ConsoleAppender

import static ch.qos.logback.classic.Level.*

final patternString = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{5} Groovy - %msg%n"


final STDOUT = "STDOUT"

appender(STDOUT, ConsoleAppender) {
  encoder(PatternLayoutEncoder) {
    pattern = patternString
  }
}

root(WARN, [STDOUT])
logger("com.amazonaws.samples.SimpleQueueServiceSample", TRACE, [STDOUT])

