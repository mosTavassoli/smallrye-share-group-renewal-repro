# Reproducer: share-group lock renewal with slow processing

One `@Incoming @Blocking` method on a share-group channel that sleeps longer than the record lock.
Each invocation is logged with the record's delivery count.

Requires Java 21, Maven, and Docker (Testcontainers starts `apache/kafka:4.2.1`).

```sh
mvn test
```

Look for the `REPRO RESULT` and `REPRO LOG` lines:

- `FastProcessingTest` (control, 0.5 s processing, 2 s lock): `first=1 second=1`.
- `SlowProcessingTest` (3 s processing, 2 s lock, renewal check every 500 ms): the first record's handler
  ran 1 to 5 times across runs; the second record was acknowledged by the broker, but its handler was not
  invoked within 45 s in any run. When the first record's handler ran more than once, the invocations did not
  overlap and the delivery count increased. After the first failure, `SRMSG18287 ... The record cannot be
  acknowledged` repeats on every renewal interval until the test ends.

Timings are scaled down so it runs in about two minutes. Similar behavior was observed with Kafka's default
30 s lock, SmallRye's default 5 s renewal-check interval, and 40 s of processing.

Observed with Quarkus 4.0.0.CR1 (smallrye-reactive-messaging-kafka 5.0.0-RC2, kafka-clients 4.2.2),
and separately with Quarkus 3.40.1.2 (smallrye-reactive-messaging-kafka 4.37.0, kafka-clients 4.2.1).
