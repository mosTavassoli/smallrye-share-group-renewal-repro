package repro;

import static org.assertj.core.api.Assertions.assertThat;

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
 * Control: processing (0.5 s) is far below the record lock (10 s), so the connector never starts renewing.
 * With a 2 s lock, renewal can start while a fast record is still queued, and the same failure was observed
 * intermittently (see README), so a short lock is not a clean control.
 */
@QuarkusTest
@TestProfile(FastProcessingTest.Profile.class)
@WithTestResource(value = KafkaResource.class, scope = TestResourceScope.RESTRICTED_TO_CLASS,
        initArgs = @ResourceArg(name = "lockMs", value = "10000"))
class FastProcessingTest {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrap;

    @Test
    void fastRecordThenSecondRecord() throws Exception {
        var first = Probe.invocations(bootstrap, "first", Duration.ofSeconds(10), Duration.ofSeconds(120));
        var second = Probe.invocations(bootstrap, "second", Duration.ofSeconds(10), Duration.ofSeconds(45));
        System.out.println("REPRO RESULT control: first " + first + ", second " + second);
        // The control must be able to fail: each record is handled exactly once, to completion.
        assertThat(first).isEqualTo(new Probe.Observation(1, 1, true));
        assertThat(second).isEqualTo(new Probe.Observation(1, 1, true));
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("repro.processing-ms", "500",
                    "mp.messaging.incoming.orders.auto.commit.interval.ms", "500");
        }
    }
}
