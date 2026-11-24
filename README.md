# Reproducer: share-group lock renewal with slow processing

One `@Incoming @Blocking` method on a share-group channel that sleeps longer than the record lock.

Requires Java 21, Maven, and Docker (Testcontainers starts `apache/kafka:4.2.1`).

```sh
mvn test
```

Look for the two `REPRO RESULT` lines:

- `FastProcessingTest` (control, 0.5 s processing, 2 s lock): `first=1 second=1`
- `SlowProcessingTest` (3 s processing, 2 s lock, renewal every 500 ms): observed `first=5 second=0` —
  the first record is processed repeatedly and the second record is never processed.

Timings are scaled down so it runs in about two minutes; the same behavior was observed with
Kafka's default 30 s lock, SmallRye's default 5 s renewal interval, and 40 s of processing.

Observed with Quarkus 4.0.0.CR1 (smallrye-reactive-messaging-kafka 5.0.0-RC2, kafka-clients 4.2.2),
and separately with Quarkus 3.40.1.2 (smallrye-reactive-messaging-kafka 4.37.0, kafka-clients 4.2.1).
