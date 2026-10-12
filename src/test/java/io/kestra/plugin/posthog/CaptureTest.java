package io.kestra.plugin.posthog;

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class CaptureTest {
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
    void capture_sendsEventToBatchEndpoint() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        capture("user-1", "pipeline_completed", Property.ofValue(Map.of("pipeline_name", "etl", "rows", 42)), null).run(runContext);

        // no waiting: run() must only return once the request has been delivered
        ReceivedRequest request = server.requests.poll();
        assertNotNull(request, "no request reached the server before run() returned");
        assertEquals("POST", request.method());
        assertEquals("/batch", request.path());

        Map<String, Object> body = parse(request.body());
        assertEquals(API_KEY, body.get("api_key"));

        Map<String, Object> event = singleEvent(body);
        assertEquals("pipeline_completed", event.get("event"));
        assertEquals("user-1", event.get("distinct_id"));

        Map<String, Object> properties = map(event.get("properties"));
        assertEquals("etl", properties.get("pipeline_name"));
        assertEquals(42, properties.get("rows"));

        assertTrue(server.requests.isEmpty(), "expected exactly one request");
    }

    @Test
    void capture_rendersTemplatedProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("execution", Map.of("id", "exec123")));

        // Property.ofValue() never renders, so build the property from JSON like Kestra does when it reads a flow
        Property<Map<String, Object>> templatedProperties = JacksonMapper.ofJson().readValue(
            "{\"kestra_execution_id\": \"{{ execution.id }}\"}",
            new TypeReference<Property<Map<String, Object>>>() {
            }
        );

        capture("user-1", "templated", templatedProperties, null).run(runContext);

        ReceivedRequest request = server.requests.poll();
        assertNotNull(request, "no request reached the server before run() returned");

        Map<String, Object> properties = map(singleEvent(parse(request.body())).get("properties"));
        assertEquals("exec123", properties.get("kestra_execution_id"));
    }

    @Test
    void capture_sendsTimestampWhenProvided() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        Instant timestamp = Instant.parse("2026-01-01T00:00:00Z");

        capture("user-1", "with_timestamp", null, timestamp).run(runContext);

        ReceivedRequest request = server.requests.poll();
        assertNotNull(request, "no request reached the server before run() returned");

        Map<String, Object> event = singleEvent(parse(request.body()));
        assertEquals(timestamp, Instant.parse((String) event.get("timestamp")));
    }

    @Test
    void capture_waitsForFlushBeforeReturning() throws Exception {
        server.responseDelayMillis = 700;
        RunContext runContext = runContextFactory.of(Map.of());

        long start = System.nanoTime();
        capture("user-1", "slow_server", null, null).run(runContext);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // the flag is set right before the server sends its response, so if run() has returned,
        // it can only have done so after the HTTP exchange was completed
        assertTrue(server.responseSent.get(), "run() returned before the server sent its response");
        assertTrue(elapsedMillis >= 600, "run() returned after only " + elapsedMillis + "ms, it did not wait for the delayed response");
        assertNotNull(server.requests.poll(), "no request reached the server before run() returned");
    }

    @Test
    void capture_rejectsBlankDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());

        assertThrows(IllegalArgumentException.class, () -> capture("   ", "some_event", null, null).run(runContext));

        assertTrue(server.requests.isEmpty(), "no request should be sent for a blank distinctId");
    }

    @Test
    void capture_rejectsBlankEvent() {
        RunContext runContext = runContextFactory.of(Map.of());

        assertThrows(IllegalArgumentException.class, () -> capture("user-1", "   ", null, null).run(runContext));

        assertTrue(server.requests.isEmpty(), "no request should be sent for a blank event");
    }

    private Capture capture(String distinctId, String event, Property<Map<String, Object>> properties, Instant timestamp) {
        return Capture.builder()
            .id("capture")
            .type(Capture.class.getName())
            .apiKey(Property.ofValue(API_KEY))
            .host(Property.ofValue(server.url()))
            .distinctId(Property.ofValue(distinctId))
            .event(Property.ofValue(event))
            .properties(properties)
            .timestamp(timestamp == null ? null : Property.ofValue(timestamp))
            .build();
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
    private static Map<String, Object> singleEvent(Map<String, Object> body) {
        List<Map<String, Object>> batch = (List<Map<String, Object>>) body.get("batch");
        assertEquals(1, batch.size(), "expected a single event in the batch");
        return batch.getFirst();
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

        String url() {
            return "http://127.0.0.1:" + httpServer.getAddress().getPort();
        }

        void stop() {
            httpServer.stop(0);
        }
    }
}
