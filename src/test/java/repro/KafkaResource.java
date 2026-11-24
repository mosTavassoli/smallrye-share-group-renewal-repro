package repro;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.testcontainers.kafka.KafkaContainer;

/** Single Kafka 4.2.1 broker (share groups on by default) with a configurable record lock for group "repro". */
public class KafkaResource implements QuarkusTestResourceLifecycleManager {

    private KafkaContainer kafka;
    private String lockMs = "30000";

    @Override
    public void init(Map<String, String> args) {
        lockMs = args.getOrDefault("lockMs", lockMs);
    }

    @Override
    public Map<String, String> start() {
        kafka = new KafkaContainer("apache/kafka:4.2.1")
                .withEnv("KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS", "1000")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");
        kafka.start();
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic("orders", 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP, "repro"), List.of(
                    new AlterConfigOp(new ConfigEntry("share.auto.offset.reset", "earliest"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry("share.record.lock.duration.ms", lockMs), AlterConfigOp.OpType.SET))))
                    .all().get(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return Map.of("kafka.bootstrap.servers", kafka.getBootstrapServers());
    }

    @Override
    public void stop() {
        if (kafka != null) {
            kafka.stop();
        }
    }
}
