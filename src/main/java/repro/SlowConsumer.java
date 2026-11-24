package repro;

import io.smallrye.reactive.messaging.annotations.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;

/** Processes each record slowly (standing in for a slow downstream call) and counts invocations per payload. */
@ApplicationScoped
public class SlowConsumer {

    public static final Map<String, AtomicInteger> INVOCATIONS = new ConcurrentHashMap<>();

    @ConfigProperty(name = "repro.processing-ms")
    long processingMs;

    @Incoming("orders")
    @Blocking
    public void consume(String payload) throws InterruptedException {
        INVOCATIONS.computeIfAbsent(payload, p -> new AtomicInteger()).incrementAndGet();
        Thread.sleep(processingMs);
    }
}
