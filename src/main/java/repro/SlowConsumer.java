package repro;

import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;

/** Processes each record slowly (standing in for a slow downstream call) and records every invocation. */
@ApplicationScoped
public class SlowConsumer {

    public static final Map<String, AtomicInteger> INVOCATIONS = new ConcurrentHashMap<>();
    public static final Map<String, AtomicInteger> COMPLETED = new ConcurrentHashMap<>();
    public static final List<String> LOG = new CopyOnWriteArrayList<>();

    @ConfigProperty(name = "repro.processing-ms")
    long processingMs;

    @Incoming("orders")
    @Blocking
    public void consume(String payload, IncomingKafkaRecordMetadata<String, String> metadata)
            throws InterruptedException {
        INVOCATIONS.computeIfAbsent(payload, p -> new AtomicInteger()).incrementAndGet();
        int deliveryCount = metadata.getRecord().deliveryCount().map(Short::intValue).orElse(-1);
        long start = System.currentTimeMillis();
        LOG.add(String.format("%d start payload=%s offset=%d deliveryCount=%d thread=%s",
                start, payload, metadata.getOffset(), deliveryCount, Thread.currentThread().getName()));
        Thread.sleep(processingMs);
        COMPLETED.computeIfAbsent(payload, p -> new AtomicInteger()).incrementAndGet();
        LOG.add(String.format("%d end   payload=%s offset=%d deliveryCount=%d thread=%s",
                System.currentTimeMillis(), payload, metadata.getOffset(), deliveryCount,
                Thread.currentThread().getName()));
    }
}
