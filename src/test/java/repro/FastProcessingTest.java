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

/** Control: processing (0.5 s) stays under the record lock (2 s). */
@QuarkusTest
@TestProfile(FastProcessingTest.Profile.class)
@WithTestResource(value = KafkaResource.class, scope = TestResourceScope.RESTRICTED_TO_CLASS,
        initArgs = @ResourceArg(name = "lockMs", value = "2000"))
class FastProcessingTest {

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrap;

    @Test
    void fastRecordThenSecondRecord() throws Exception {
        int first = Probe.invocations(bootstrap, "first", Duration.ofSeconds(10), Duration.ofSeconds(120));
        int second = Probe.invocations(bootstrap, "second", Duration.ofSeconds(10), Duration.ofSeconds(45));
        System.out.println("REPRO RESULT control: first=" + first + " second=" + second);
    }

    public static class Profile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("repro.processing-ms", "500",
                    "mp.messaging.incoming.orders.auto.commit.interval.ms", "500");
        }
    }
}
