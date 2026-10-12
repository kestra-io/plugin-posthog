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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class IdentifyTest {
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
    void identify_sendsIdentifyEventToBatchEndpoint() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        identify("user-1", properties("{\"email\": \"a@b.c\", \"plan\": \"pro\"}")).run(runContext);

        // no waiting: run() must only return once the request has been delivered
        ReceivedRequest request = server.requests.poll();
        assertNotNull(request, "no request reached the server before run() returned");
        assertEquals("POST", request.method());
        assertEquals("/batch", request.path());

        Map<String, Object> body = parse(request.body());
        assertEquals(API_KEY, body.get("api_key"));

        Map<String, Object> event = singleEvent(body);
        assertEquals("$identify", event.get("event"));
        assertEquals("user-1", event.get("distinct_id"));

        Map<String, Object> set = map(map(event.get("properties")).get("$set"));
        assertEquals("a@b.c", set.get("email"));
        assertEquals("pro", set.get("plan"));
        assertEquals(2, set.size());

        assertTrue(server.requests.isEmpty(), "expected exactly one request");
    }

    @Test
    void identify_rendersTemplatedDistinctIdAndProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("user", "user-42", "plan", "enterprise", "team", "data"));

        // Property.ofValue() never renders, so build the property from JSON like Kestra does when it reads a flow
        Identify identify = identify(text("{{ user }}"), properties("""
            {"plan": "{{ plan }}", "email": "{{ user }}@example.com", "nested": {"team": "{{ team }}"}}"""));
        identify.run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals("user-42", event.get("distinct_id"));

        Map<String, Object> set = map(map(event.get("properties")).get("$set"));
        assertEquals("enterprise", set.get("plan"));
        assertEquals("user-42@example.com", set.get("email"));
        assertEquals("data", map(set.get("nested")).get("team"));
    }

    @Test
    void identify_preservesNestedAndTypedValues() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        identify("user-1", properties("""
            {"age": 42, "score": 1.5, "active": true, "address": {"city": "Paris", "geo": {"lat": 48, "lon": 2}}, "tags": ["a", "b"]}"""))
            .run(runContext);

        Map<String, Object> set = map(map(singleEvent(parse(server.requests.poll().body())).get("properties")).get("$set"));
        assertEquals(42, set.get("age"));
        assertEquals(1.5, set.get("score"));
        assertEquals(true, set.get("active"));
        assertEquals("Paris", map(set.get("address")).get("city"));
        assertEquals(48, map(map(set.get("address")).get("geo")).get("lat"));
        assertEquals(List.of("a", "b"), set.get("tags"));
    }

    @Test
    void identify_rejectsBlankDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> identify("   ", properties("{\"plan\": \"pro\"}")).run(runContext));

        assertTrue(exception.getMessage().contains("distinctId"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent for a blank distinctId");
    }

    @Test
    void identify_rejectsMissingDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> identify((String) null, properties("{\"plan\": \"pro\"}")).run(runContext));

        assertTrue(exception.getMessage().contains("distinctId"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing distinctId");
    }

    @Test
    void identify_rejectsMissingOrEmptyProperties() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException missing = assertThrows(IllegalArgumentException.class, () -> identify("user-1", null).run(runContext));
        IllegalArgumentException empty = assertThrows(IllegalArgumentException.class, () -> identify("user-1", properties("{}")).run(runContext));

        assertTrue(missing.getMessage().contains("properties"), missing.getMessage());
        assertTrue(empty.getMessage().contains("properties"), empty.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent without properties");
    }

    @Test
    void identify_rejectsBlankPropertyName() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException emptyName = assertThrows(IllegalArgumentException.class,
            () -> identify("user-1", properties("{\"\": \"something\", \"plan\": \"pro\"}")).run(runContext));
        IllegalArgumentException spacesName = assertThrows(IllegalArgumentException.class,
            () -> identify("user-1", properties("{\"   \": \"something\"}")).run(runContext));

        assertTrue(emptyName.getMessage().contains("blank property name"), emptyName.getMessage());
        assertTrue(spacesName.getMessage().contains("blank property name"), spacesName.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent for a blank property name");
    }

    @Test
    void identify_omitsNullPropertyValues() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        identify("user-1", properties("{\"plan\": \"pro\", \"cleared\": null}")).run(runContext);

        Map<String, Object> set = map(map(singleEvent(parse(server.requests.poll().body())).get("properties")).get("$set"));
        assertEquals("pro", set.get("plan"));
        assertFalse(set.containsKey("cleared"), "a null value must not appear in $set, and does not unset the property");
    }

    @Test
    void identify_waitsForFlushBeforeReturning() throws Exception {
        server.responseDelayMillis = 700;
        RunContext runContext = runContextFactory.of(Map.of());

        long start = System.nanoTime();
        identify("user-1", properties("{\"plan\": \"pro\"}")).run(runContext);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // the flag is set right before the server sends its response, so if run() has returned,
        // it can only have done so after the HTTP exchange was completed
        assertTrue(server.responseSent.get(), "run() returned before the server sent its response");
        assertTrue(elapsedMillis >= 600, "run() returned after only " + elapsedMillis + "ms, it did not wait for the delayed response");
        assertNotNull(server.requests.poll(), "no request reached the server before run() returned");
    }

    @Test
    void identify_rejectsBlankApiKey() {
        RunContext runContext = runContextFactory.of(Map.of());

        Identify identify = Identify.builder()
            .id("identify")
            .type(Identify.class.getName())
            .apiKey(Property.ofValue("   "))
            .host(Property.ofValue(server.url()))
            .distinctId(Property.ofValue("user-1"))
            .properties(properties("{\"plan\": \"pro\"}"))
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> identify.run(runContext));

        assertTrue(exception.getMessage().contains("apiKey"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent without an API key");
    }

    private Identify identify(String distinctId, Property<Map<String, Object>> properties) {
        return identify(distinctId == null ? null : Property.ofValue(distinctId), properties);
    }

    private Identify identify(Property<String> distinctId, Property<Map<String, Object>> properties) {
        return Identify.builder()
            .id("identify")
            .type(Identify.class.getName())
            .apiKey(Property.ofValue(API_KEY))
            .host(Property.ofValue(server.url()))
            .distinctId(distinctId)
            .properties(properties)
            .build();
    }

    /**
     * Builds a string property from JSON, like Kestra does when it reads a flow, so that templates are really rendered.
     */
    private static Property<String> text(String value) {
        try {
            return JacksonMapper.ofJson().readValue(JacksonMapper.ofJson().writeValueAsString(value), new TypeReference<Property<String>>() {
            });
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Builds the property from JSON, like Kestra does when it reads a flow, so that templates are really rendered.
     */
    private static Property<Map<String, Object>> properties(String json) {
        try {
            return JacksonMapper.ofJson().readValue(json, new TypeReference<Property<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
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
