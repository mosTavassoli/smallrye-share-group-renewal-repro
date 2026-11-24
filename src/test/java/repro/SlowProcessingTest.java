package repro;

import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.common.TestResourceScope;
import io.quarkus.test.common.WithTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.time.Duration;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

/** Processing (3 s) outlasts the record lock (2 s); SmallRye renews every 500 ms; default 60 s max age. */
@QuarkusTest
@TestProfile(SlowProcessingTest.Profile.class)
@WithTestResource(value = KafkaResource.class, scope = TestResourceScope.RESTRICTED_TO_CLASS,
        initArgs = @ResourceArg(name = "lockMs", value = "2000"))
class SlowProcessingTest {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrap;

    @Test
    void slowRecordThenSecondRecord() throws Exception {
        int first = Probe.invocations(bootstrap, "first", Duration.ofSeconds(10), Duration.ofSeconds(120));
        int second = Probe.invocations(bootstrap, "second", Duration.ofSeconds(10), Duration.ofSeconds(45));
        System.out.println("REPRO RESULT slow: first=" + first + " second=" + second);
        long t0 = SlowConsumer.LOG.isEmpty() ? 0 : Long.parseLong(SlowConsumer.LOG.get(0).split(" ")[0]);
        SlowConsumer.LOG.forEach(line -> {
            String[] parts = line.split(" ", 2);
            System.out.printf("REPRO LOG +%5d ms %s%n", Long.parseLong(parts[0]) - t0, parts[1]);
        });
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("repro.processing-ms", "3000",
                    "mp.messaging.incoming.orders.auto.commit.interval.ms", "500");
        }
    }
}
