package io.kestra.plugin.posthog;

import com.sun.net.httpserver.HttpServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class CaptureBatchTest {
    private static final String API_KEY = "phc_dummy";

    @Inject
    private RunContextFactory runContextFactory;

    private MockPosthogServer server;

    @BeforeEach
    void startServer() throws Exception {
        server = new MockPosthogServer();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void captureBatch_sendsBatchSuccessfully() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        CaptureBatch.Output output = batch(List.of(
            event("user-1", "first", Map.of("a", 1)),
            event("user-2", "second", Map.of("b", "two")),
            event("user-3", "third", null)
        ), null).run(runContext);

        // no waiting: run() must only return once everything has been delivered
        List<ReceivedRequest> requests = server.drain();
        assertEquals(1, requests.size(), "3 events must be sent in a single request");
        assertEquals("POST", requests.getFirst().method());
        assertEquals("/batch", requests.getFirst().path());

        Map<String, Object> body = parse(requests.getFirst().body());
        assertEquals(API_KEY, body.get("api_key"));

        List<Map<String, Object>> events = events(requests);
        assertEquals(List.of("first", "second", "third"), events.stream().map(e -> e.get("event")).toList());
        assertEquals(List.of("user-1", "user-2", "user-3"), events.stream().map(e -> e.get("distinct_id")).toList());
        assertEquals(1, map(events.get(0).get("properties")).get("a"));
        assertEquals("two", map(events.get(1).get("properties")).get("b"));
        assertEquals(3, output.getEventsCount());
    }

    @Test
    void captureBatch_sendsAllEventsWhenCountExceedsDefaultFlushThreshold() throws Exception {
        RunContext runContextWith250 = runContextFactory.of(Map.of());
        List<Map<String, Object>> input = new ArrayList<>();
        for (int i = 0; i < 250; i++) {
            input.add(event("user-" + (i % 7), "event_" + i, null));
        }

        CaptureBatch.Output output = batch(input, null).run(runContextWith250);

        // the SDK defaults (flushAt=100) would return early and lose the last 50 events
        List<ReceivedRequest> requests = server.drain();
        List<Map<String, Object>> events = events(requests);
        assertEquals(250, events.size(), "all events must have been delivered before run() returned");
        assertEquals(250, events.stream().map(e -> e.get("event")).distinct().count());
        assertEquals(250, output.getEventsCount());
    }

    @Test
    void captureBatch_splitsRequestsAccordingToBatchSize() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        List<Map<String, Object>> input = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            input.add(event("user-1", "event_" + i, null));
        }

        CaptureBatch.Output output = batch(input, 10).run(runContext);

        List<ReceivedRequest> requests = server.drain();
        assertEquals(List.of(10, 10, 5), requests.stream().map(r -> eventsOf(r).size()).toList());
        assertTrue(requests.stream().allMatch(r -> r.path().equals("/batch")));
        assertEquals(25, events(requests).size());
        assertEquals(25, output.getEventsCount());
    }

    @Test
    void captureBatch_rendersTemplatesInEventFields() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("execution", Map.of("id", "exec123"), "name", "pipe1"));

        batch(List.of(
            event("{{ name }}", "completed_{{ name }}", Map.of("kestra_execution_id", "{{ execution.id }}", "nested", Map.of("pipeline", "{{ name }}")))
        ), null).run(runContext);

        Map<String, Object> event = events(server.drain()).getFirst();
        assertEquals("pipe1", event.get("distinct_id"));
        assertEquals("completed_pipe1", event.get("event"));
        Map<String, Object> properties = map(event.get("properties"));
        assertEquals("exec123", properties.get("kestra_execution_id"));
        assertEquals("pipe1", map(properties.get("nested")).get("pipeline"));
    }

    @Test
    void captureBatch_sendsTimestampWhenProvided() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Instant expected = Instant.parse("2026-01-01T00:00:00Z");

        Map<String, Object> withString = event("user-1", "as_string", null);
        withString.put("timestamp", "2026-01-01T00:00:00Z");
        Map<String, Object> withOffset = event("user-1", "with_offset", null);
        withOffset.put("timestamp", "2026-01-01T02:00:00+02:00");
        Map<String, Object> withInstant = event("user-1", "as_instant", null);
        withInstant.put("timestamp", expected);

        batch(List.of(withString, withOffset, withInstant), null).run(runContext);

        List<Map<String, Object>> events = events(server.drain());
        assertEquals(3, events.size());
        for (Map<String, Object> event : events) {
            assertEquals(expected, Instant.parse((String) event.get("timestamp")), "wrong timestamp for " + event.get("event"));
        }
    }

    @Test
    void captureBatch_acceptsOffsetAndZonedDateTimeTimestamps() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Instant expected = Instant.parse("2026-01-01T00:00:00Z");

        Map<String, Object> withOffsetDateTime = event("user-1", "offset_date_time", null);
        withOffsetDateTime.put("timestamp", OffsetDateTime.parse("2026-01-01T02:00:00+02:00"));
        Map<String, Object> withZonedDateTime = event("user-1", "zoned_date_time", null);
        withZonedDateTime.put("timestamp", ZonedDateTime.parse("2026-01-01T01:00:00+01:00[Europe/Paris]"));

        CaptureBatch.Output output = batch(List.of(withOffsetDateTime, withZonedDateTime), null).run(runContext);

        List<Map<String, Object>> events = events(server.drain());
        assertEquals(2, events.size());
        for (Map<String, Object> event : events) {
            assertEquals(expected, Instant.parse((String) event.get("timestamp")), "wrong timestamp for " + event.get("event"));
        }
        assertEquals(2, output.getEventsCount());
    }

    @Test
    void captureBatch_rejectsTimestampsWithoutZone() {
        RunContext runContext = runContextFactory.of(Map.of());
        Map<String, Object> localDateTime = event("user-1", "local_date_time", null);
        localDateTime.put("timestamp", LocalDateTime.parse("2026-01-01T00:00:00"));
        Map<String, Object> localDate = event("user-1", "local_date", null);
        localDate.put("timestamp", LocalDate.parse("2026-01-01"));
        Map<String, Object> bool = event("user-1", "boolean", null);
        bool.put("timestamp", true);

        for (Map<String, Object> invalid : List.of(localDateTime, localDate, bool)) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(List.of(invalid), null).run(runContext));
            assertTrue(exception.getMessage().contains("index 0"), exception.getMessage());
            assertTrue(exception.getMessage().contains("timestamp"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty());
    }

    @Test
    void captureBatch_waitsForFlushBeforeReturning() throws Exception {
        server.responseDelayMillis = 700;
        RunContext runContext = runContextFactory.of(Map.of());

        long start = System.nanoTime();
        batch(List.of(event("user-1", "slow_1", null), event("user-1", "slow_2", null)), null).run(runContext);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // the flag is set right before the server sends its response, so if run() has returned,
        // it can only have done so after the HTTP exchange was completed
        assertTrue(server.responseSent.get(), "run() returned before the server sent its response");
        assertTrue(elapsedMillis >= 600, "run() returned after only " + elapsedMillis + "ms, it did not wait for the delayed response");
        assertEquals(2, events(server.drain()).size());
    }

    @Test
    void captureBatch_rejectsBlankDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(List.of(
            event("user-1", "ok", null),
            event("   ", "blank_distinct_id", null)
        ), null).run(runContext));

        assertTrue(exception.getMessage().contains("index 1"), exception.getMessage());
        assertTrue(exception.getMessage().contains("distinctId"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "nothing must be sent when an event is invalid");
    }

    @Test
    void captureBatch_rejectsMissingDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());
        Map<String, Object> missing = new LinkedHashMap<>();
        missing.put("event", "no_distinct_id");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(List.of(missing), null).run(runContext));

        assertTrue(exception.getMessage().contains("index 0"), exception.getMessage());
        assertTrue(exception.getMessage().contains("distinctId"), exception.getMessage());
        assertTrue(server.requests.isEmpty());
    }

    @Test
    void captureBatch_rejectsBlankEvent() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(List.of(
            event("user-1", "   ", null)
        ), null).run(runContext));

        assertTrue(exception.getMessage().contains("index 0"), exception.getMessage());
        assertTrue(exception.getMessage().contains("`event`"), exception.getMessage());
        assertTrue(server.requests.isEmpty());
    }

    @Test
    void captureBatch_rejectsMissingEvent() {
        RunContext runContext = runContextFactory.of(Map.of());
        Map<String, Object> missing = new LinkedHashMap<>();
        missing.put("distinctId", "user-1");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(List.of(missing), null).run(runContext));

        assertTrue(exception.getMessage().contains("`event`"), exception.getMessage());
        assertTrue(server.requests.isEmpty());
    }

    @Test
    void captureBatch_rejectsInvalidPropertiesType() {
        RunContext runContext = runContextFactory.of(Map.of());
        Map<String, Object> invalid = event("user-1", "bad_properties", null);
        invalid.put("properties", "not an object");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(List.of(invalid), null).run(runContext));

        assertTrue(exception.getMessage().contains("index 0"), exception.getMessage());
        assertTrue(exception.getMessage().contains("properties"), exception.getMessage());
        assertTrue(server.requests.isEmpty());
    }

    @Test
    void captureBatch_rejectsInvalidTimestamp() {
        RunContext runContext = runContextFactory.of(Map.of());
        Map<String, Object> notADate = event("user-1", "bad_timestamp", null);
        notADate.put("timestamp", "not-a-date");
        Map<String, Object> numeric = event("user-1", "numeric_timestamp", null);
        numeric.put("timestamp", 1767225600000L);

        IllegalArgumentException notADateException = assertThrows(IllegalArgumentException.class, () -> batch(List.of(notADate), null).run(runContext));
        IllegalArgumentException numericException = assertThrows(IllegalArgumentException.class, () -> batch(List.of(numeric), null).run(runContext));

        assertTrue(notADateException.getMessage().contains("timestamp"), notADateException.getMessage());
        assertTrue(numericException.getMessage().contains("timestamp"), numericException.getMessage());
        assertTrue(server.requests.isEmpty());
    }

    @Test
    void captureBatch_validatesAllEventsBeforeSendingAnything() {
        RunContext runContext = runContextFactory.of(Map.of());
        List<Map<String, Object>> input = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            input.add(event("user-1", "valid_" + i, null));
        }
        input.add(event("user-1", "   ", null)); // only the last one is invalid

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(input, null).run(runContext));

        assertTrue(exception.getMessage().contains("index 5"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "the 5 valid events must not be sent when a later one is invalid");
    }

    @Test
    void captureBatch_emptyInputSendsNothing() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        CaptureBatch.Output output = batch(List.of(), null).run(runContext);

        assertEquals(0, output.getEventsCount());
        assertTrue(server.requests.isEmpty(), "no request must be sent for an empty input");
    }

    @Test
    void captureBatch_rejectsNonPositiveBatchSize() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> batch(List.of(event("user-1", "e", null)), 0).run(runContext));

        assertTrue(exception.getMessage().contains("batchSize"), exception.getMessage());
        assertTrue(server.requests.isEmpty());
    }

    @Test
    void captureBatch_ignoresUnknownKeysAndAllowsDollarPrefixedEvents() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Map<String, Object> withExtra = event("user-1", "$pageview", null);
        withExtra.put("some_other_column", "ignored");

        CaptureBatch.Output output = batch(List.of(withExtra), null).run(runContext);

        Map<String, Object> event = events(server.drain()).getFirst();
        assertEquals("$pageview", event.get("event"));
        assertTrue(!map(event.get("properties")).containsKey("some_other_column"));
        assertEquals(1, output.getEventsCount());
    }

    private CaptureBatch batch(Object from, Integer batchSize) {
        CaptureBatch.CaptureBatchBuilder<?, ?> builder = CaptureBatch.builder()
            .id("capture_batch")
            .type(CaptureBatch.class.getName())
            .apiKey(Property.ofValue(API_KEY))
            .host(Property.ofValue(server.url()))
            .from(from);
        if (batchSize != null) {
            builder.batchSize(Property.ofValue(batchSize));
        }
        return builder.build();
    }

    private static Map<String, Object> event(String distinctId, String event, Map<String, Object> properties) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("distinctId", distinctId);
        item.put("event", event);
        if (properties != null) {
            item.put("properties", properties);
        }
        return item;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(String json) throws Exception {
        return JacksonMapper.ofJson().readValue(json, Map.class);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> eventsOf(ReceivedRequest request) {
        try {
            return (List<Map<String, Object>>) parse(request.body()).get("batch");
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<Map<String, Object>> events(List<ReceivedRequest> requests) {
        List<Map<String, Object>> events = new ArrayList<>();
        requests.forEach(request -> events.addAll(eventsOf(request)));
        return events;
    }

    private record ReceivedRequest(String method, String path, String body) {
    }

    /**
     * Minimal local stand-in for the PostHog ingestion API: records every request (gzip bodies are decompressed)
     * and answers {@code 200}.
     */
    private static class MockPosthogServer {
        final BlockingQueue<ReceivedRequest> requests = new LinkedBlockingQueue<>();
        final AtomicBoolean responseSent = new AtomicBoolean(false);
        volatile long responseDelayMillis = 0;

        private final HttpServer httpServer;

        MockPosthogServer() throws Exception {
            httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            httpServer.createContext("/", exchange -> {
                byte[] raw = exchange.getRequestBody().readAllBytes();
                String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
                byte[] decoded = encoding != null && encoding.contains("gzip")
                    ? new GZIPInputStream(new ByteArrayInputStream(raw)).readAllBytes()
                    : raw;

                requests.add(new ReceivedRequest(
                    exchange.getRequestMethod(),
                    exchange.getRequestURI().getPath(),
                    new String(decoded, StandardCharsets.UTF_8)
                ));

                if (responseDelayMillis > 0) {
                    try {
                        Thread.sleep(responseDelayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                byte[] response = "{\"status\":\"Ok\"}".getBytes(StandardCharsets.UTF_8);
                responseSent.set(true);
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
            });
            httpServer.start();
        }

        List<ReceivedRequest> drain() {
            List<ReceivedRequest> drained = new ArrayList<>();
            requests.drainTo(drained);
            return drained;
        }

        String url() {
            return "http://127.0.0.1:" + httpServer.getAddress().getPort();
        }

        void stop() {
            httpServer.stop(0);
        }
    }
}
