package repro;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Sends one payload and observes how SlowConsumer handles it. */
final class Probe {

    /**
     * Handler invocations started and completed, and whether handling settled (started at least once, every
     * invocation completed, nothing changed for the quiet period) before the time limit.
     */
    record Observation(int started, int completed, boolean settled) {
        @Override
        public String toString() {
            return "started=" + started + " completed=" + completed + (settled ? " (settled)" : " (not settled in time)");
        }
    }

    private Probe() {
    }

    static Observation invocations(String bootstrap, String payload, Duration quiet, Duration maxWait)
            throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", bootstrap,
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>("orders", payload, payload)).get(20, TimeUnit.SECONDS);
        }
        long deadline = System.nanoTime() + maxWait.toNanos();
        int lastStarted = -1;
        int lastCompleted = -1;
        long lastChange = System.nanoTime();
        while (System.nanoTime() < deadline) {
            int started = count(SlowConsumer.INVOCATIONS, payload);
            int completed = count(SlowConsumer.COMPLETED, payload);
            if (started != lastStarted || completed != lastCompleted) {
                lastStarted = started;
                lastCompleted = completed;
                lastChange = System.nanoTime();
            } else if (started > 0 && started == completed && System.nanoTime() - lastChange > quiet.toNanos()) {
                return new Observation(started, completed, true);
            }
            Thread.sleep(200);
        }
        return new Observation(Math.max(lastStarted, 0), Math.max(lastCompleted, 0), false);
    }

    private static int count(Map<String, AtomicInteger> counts, String payload) {
        AtomicInteger value = counts.get(payload);
        return value == null ? 0 : value.get();
    }
}
