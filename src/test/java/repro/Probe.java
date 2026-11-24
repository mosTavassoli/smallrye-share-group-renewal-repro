package repro;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Sends one payload and returns how many times SlowConsumer processed it once things go quiet. */
final class Probe {

    private Probe() {
    }

    static int invocations(String bootstrap, String payload, Duration quiet, Duration maxWait) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", bootstrap,
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>("orders", payload, payload)).get(20, TimeUnit.SECONDS);
        }
        long deadline = System.nanoTime() + maxWait.toNanos();
        int last = -1;
        long lastChange = System.nanoTime();
        while (System.nanoTime() < deadline) {
            AtomicInteger count = SlowConsumer.INVOCATIONS.get(payload);
            int now = count == null ? 0 : count.get();
            if (now != last) {
                last = now;
                lastChange = System.nanoTime();
            } else if (now > 0 && System.nanoTime() - lastChange > quiet.toNanos()) {
                return now;
            }
            Thread.sleep(200);
        }
        return last;
    }
}
