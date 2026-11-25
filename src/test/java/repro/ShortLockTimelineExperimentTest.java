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

/**
 * Experiment, not a regression test: fast processing (0.5 s) with a short lock (2 s), the setup where the control
 * failed intermittently. Prints the handler timeline; the connector's own debug log shows when it received and
 * renewed records. Only runs when -Drepro.experiment=true.
 */
@QuarkusTest
@TestProfile(ShortLockTimelineExperimentTest.Profile.class)
@WithTestResource(value = KafkaResource.class, scope = TestResourceScope.RESTRICTED_TO_CLASS,
        initArgs = @ResourceArg(name = "lockMs", value = "2000"))
@org.junit.jupiter.api.condition.EnabledIfSystemProperty(named = "repro.experiment", matches = "true")
class ShortLockTimelineExperimentTest {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrap;

    @Test
    void timeline() throws Exception {
        long t0 = System.currentTimeMillis();
        System.out.println("TIMELINE t0=" + t0);
        var first = Probe.invocations(bootstrap, "first", Duration.ofSeconds(10), Duration.ofSeconds(120));
        System.out.println("TIMELINE sent+settled first " + first + " at " + (System.currentTimeMillis() - t0));
        System.out.println("TIMELINE second sent at " + java.time.LocalTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss,SSS")));
        var second = Probe.invocations(bootstrap, "second", Duration.ofSeconds(10), Duration.ofSeconds(45));
        System.out.println("TIMELINE RESULT first " + first + ", second " + second);
        for (String line : SlowConsumer.LOG) {
            long at = Long.parseLong(line.substring(0, line.indexOf(' ')));
            String clock = java.time.LocalTime.ofInstant(java.time.Instant.ofEpochMilli(at), java.time.ZoneId.systemDefault())
                    .format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss,SSS"));
            System.out.println("TIMELINE handler " + clock + " " + line.substring(line.indexOf(' ') + 1));
        }
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("repro.processing-ms", "500",
                    "mp.messaging.incoming.orders.auto.commit.interval.ms", "500",
                    "quarkus.log.category.\"io.smallrye.reactive.messaging.kafka\".level", "DEBUG",
                    "quarkus.log.console.format", "%d{HH:mm:ss,SSS} %-5p [%c{3.}] (%t) %s%e%n");
        }
    }
}
