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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class AliasTest {
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
    void alias_sendsCreateAliasEventToBatchEndpoint() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        alias("user-123", "anonymous-456").run(runContext);

        // no waiting: run() must only return once the request has been delivered
        ReceivedRequest request = server.requests.poll();
        assertNotNull(request, "no request reached the server before run() returned");
        assertEquals("POST", request.method());
        assertEquals("/batch", request.path());

        Map<String, Object> body = parse(request.body());
        assertEquals(API_KEY, body.get("api_key"));

        Map<String, Object> event = singleEvent(body);
        assertEquals("$create_alias", event.get("event"));
        assertEquals("user-123", event.get("distinct_id"));
        assertEquals("anonymous-456", map(event.get("properties")).get("alias"));

        assertTrue(server.requests.isEmpty(), "expected exactly one request");
    }

    @Test
    void alias_rendersTemplatedIdentifiers() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("user_id", "user-42", "anonymous_id", "anon-7"));

        // Property.ofValue() never renders, so build the properties from JSON like Kestra does when it reads a flow
        alias(text("{{ user_id }}"), text("{{ anonymous_id }}")).run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals("user-42", event.get("distinct_id"));
        assertEquals("anon-7", map(event.get("properties")).get("alias"));
    }

    @Test
    void alias_trimsWhitespace() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        alias("  user-123  ", "\tanonymous-456\n").run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals("user-123", event.get("distinct_id"));
        assertEquals("anonymous-456", map(event.get("properties")).get("alias"));
    }

    @Test
    void alias_preservesSpecialCharactersAndUtf8() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String distinctId = "user+tag@example.com";
        String alias = "id/with spaces & \"quotes\" éè 日本語 😀";

        alias(distinctId, alias).run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals(distinctId, event.get("distinct_id"));
        assertEquals(alias, map(event.get("properties")).get("alias"));
    }

    @Test
    void alias_rejectsMissingAndBlankAlias() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String invalid : new String[]{null, "", "   "}) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> alias("user-123", invalid).run(runContext));
            assertTrue(exception.getMessage().contains("`alias`"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing or blank alias");
    }

    @Test
    void alias_rejectsMissingAndBlankDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String invalid : new String[]{null, "", "   "}) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> alias(invalid, "anonymous-456").run(runContext));
            assertTrue(exception.getMessage().contains("`distinctId`"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing or blank distinctId");
    }

    @Test
    void alias_rejectsEqualIdentifiers() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> alias("same-id", "same-id").run(runContext));

        assertTrue(exception.getMessage().contains("must be different"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent for equal identifiers");
    }

    @Test
    void alias_rejectsEqualIdentifiersAfterTrimming() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> alias("  same-id", "same-id  ").run(runContext));

        assertTrue(exception.getMessage().contains("must be different"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent for identifiers that are equal after trimming");
    }

    @Test
    void alias_acceptsIdentifiersThatOnlyDifferByCase() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        alias("User-1", "user-1").run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals("User-1", event.get("distinct_id"));
        assertEquals("user-1", map(event.get("properties")).get("alias"));
    }

    @Test
    void alias_waitsForFlushBeforeReturning() throws Exception {
        server.responseDelayMillis = 700;
        RunContext runContext = runContextFactory.of(Map.of());

        long start = System.nanoTime();
        alias("user-123", "anonymous-456").run(runContext);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // the flag is set right before the server sends its response, so if run() has returned,
        // it can only have done so after the HTTP exchange was completed
        assertTrue(server.responseSent.get(), "run() returned before the server sent its response");
        assertTrue(elapsedMillis >= 600, "run() returned after only " + elapsedMillis + "ms, it did not wait for the delayed response");
        assertNotNull(server.requests.poll(), "no request reached the server before run() returned");
    }

    @Test
    void alias_rejectsBlankApiKey() {
        RunContext runContext = runContextFactory.of(Map.of());

        Alias alias = Alias.builder()
            .id("alias")
            .type(Alias.class.getName())
            .apiKey(Property.ofValue("   "))
            .host(Property.ofValue(server.url()))
            .distinctId(Property.ofValue("user-123"))
            .alias(Property.ofValue("anonymous-456"))
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> alias.run(runContext));

        assertTrue(exception.getMessage().contains("apiKey"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent without an API key");
    }

    private Alias alias(String distinctId, String alias) {
        return alias(
            distinctId == null ? null : Property.ofValue(distinctId),
            alias == null ? null : Property.ofValue(alias)
        );
    }

    private Alias alias(Property<String> distinctId, Property<String> alias) {
        return Alias.builder()
            .id("alias")
            .type(Alias.class.getName())
            .apiKey(Property.ofValue(API_KEY))
            .host(Property.ofValue(server.url()))
            .distinctId(distinctId)
            .alias(alias)
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
