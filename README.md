# Reproducer: share-group lock renewal with slow processing

One `@Incoming @Blocking` method on a share-group channel that sleeps longer than the record lock.
Each invocation is logged with the record's delivery count.

Requires Java 21, Maven, and Docker (Testcontainers starts `apache/kafka:4.2.1`).

```sh
mvn test
```

Look for the `REPRO RESULT` and `REPRO LOG` lines:

Each result reports handler invocations started and completed, and whether handling settled
(every invocation completed and nothing changed for 10 s) or did not settle within the time limit.

- `FastProcessingTest` (control, 0.5 s processing, 10 s lock, so renewal never starts) asserts that each
  record is handled exactly once and settles: `started=1 completed=1 (settled)` for both records.
  With a 2 s lock instead, this control failed intermittently: in one of two local runs, the second record
  was handled 5 times despite 0.5 s processing, with the same renewal failures. Renewal is scheduled relative
  to when the connector received the record, and a record counts as in progress until its acknowledgement is
  committed, so with a short lock renewal can start after a fast handler has finished (see the timeline
  experiment below).
- `SlowProcessingTest` (3 s processing, 2 s lock, renewal check every 500 ms) only reports: the first record's
  handler ran 1 to 5 times across runs; the second record was acknowledged by the broker, but its handler was
  not invoked within 45 s in any run (`started=0 completed=0 (not settled in time)`). When the first record's handler ran more than once, the invocations did not
  overlap and the delivery count increased. After the first failure, `SRMSG18287 ... The record cannot be
  acknowledged` repeats on every renewal interval until the test ends.

## Timeline experiment (opt-in)

`ShortLockTimelineExperimentTest` repeats the short-lock setup (0.5 s processing, 2 s lock, 500 ms renewal
interval) with debug logging for `io.smallrye.reactive.messaging.kafka`, and prints handler start and end times
in the same clock format as the log. It does not assert; it fails intermittently (1 of 9 local runs), so run it
several times:

```sh
mvn test -Dtest=ShortLockTimelineExperimentTest -Drepro.experiment=true
```

Compare the `TIMELINE handler` lines with `SRMSG18290` (renewal), `SRMSG18286` (acknowledgement committed), and
`SRMSG18287` (failure). In the failing run, a renewal counted the record as in progress after its handler had
finished but before its acknowledgement was committed, and the handler then ran again with the same delivery
count (1).

Timings are scaled down so it runs in about two minutes. Similar behavior was observed with Kafka's default
30 s lock, SmallRye's default 5 s renewal-check interval, and 40 s of processing.

Observed with Quarkus 4.0.0.CR1 (smallrye-reactive-messaging-kafka 5.0.0-RC2, kafka-clients 4.2.2),
and separately with Quarkus 3.40.1.2 (smallrye-reactive-messaging-kafka 4.37.0, kafka-clients 4.2.1).
