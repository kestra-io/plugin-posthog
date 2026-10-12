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
class GroupTest {
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
    void group_sendsGroupIdentifyEventToBatchEndpoint() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        group("user-1", "company", "acme", properties("{\"name\": \"Acme Corporation\", \"plan\": \"enterprise\"}")).run(runContext);

        // no waiting: run() must only return once the request has been delivered
        ReceivedRequest request = server.requests.poll();
        assertNotNull(request, "no request reached the server before run() returned");
        assertEquals("POST", request.method());
        assertEquals("/batch", request.path());

        Map<String, Object> body = parse(request.body());
        assertEquals(API_KEY, body.get("api_key"));

        Map<String, Object> event = singleEvent(body);
        assertEquals("$groupidentify", event.get("event"));
        assertEquals("user-1", event.get("distinct_id"));

        Map<String, Object> eventProperties = map(event.get("properties"));
        assertEquals("company", eventProperties.get("$group_type"));
        assertEquals("acme", eventProperties.get("$group_key"));
        Map<String, Object> groupSet = map(eventProperties.get("$group_set"));
        assertEquals("Acme Corporation", groupSet.get("name"));
        assertEquals("enterprise", groupSet.get("plan"));
        assertEquals(2, groupSet.size());

        assertTrue(server.requests.isEmpty(), "expected exactly one request");
    }

    @Test
    void group_rendersTemplatedFields() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("user_id", "user-42", "company_id", "acme-7", "company_name", "Acme", "city", "Paris"));

        // Property.ofValue() never renders, so build the properties from JSON like Kestra does when it reads a flow
        group(text("{{ user_id }}"), text("{{ 'comp' ~ 'any' }}"), text("{{ company_id }}"), properties("""
            {"name": "{{ company_name }}", "address": {"city": "{{ city }}"}}"""))
            .run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals("user-42", event.get("distinct_id"));

        Map<String, Object> eventProperties = map(event.get("properties"));
        assertEquals("company", eventProperties.get("$group_type"));
        assertEquals("acme-7", eventProperties.get("$group_key"));
        Map<String, Object> groupSet = map(eventProperties.get("$group_set"));
        assertEquals("Acme", groupSet.get("name"));
        assertEquals("Paris", map(groupSet.get("address")).get("city"));
    }

    @Test
    void group_preservesNestedAndTypedProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        group("user-1", "company", "acme", properties("""
            {"employees": 500, "revenue": 1.5, "active": true, "address": {"city": "Paris", "geo": {"lat": 48}}, "tags": ["a", "b"]}"""))
            .run(runContext);

        Map<String, Object> groupSet = groupSet(singleEvent(parse(server.requests.poll().body())));
        assertEquals(500, groupSet.get("employees"));
        assertEquals(1.5, groupSet.get("revenue"));
        assertEquals(true, groupSet.get("active"));
        assertEquals("Paris", map(groupSet.get("address")).get("city"));
        assertEquals(48, map(map(groupSet.get("address")).get("geo")).get("lat"));
        assertEquals(List.of("a", "b"), groupSet.get("tags"));
    }

    @Test
    void group_withoutPropertiesOmitsGroupSet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        group("user-1", "company", "acme", null).run(runContext);

        // the group is still created: the event is sent, just without `$group_set`
        Map<String, Object> eventProperties = map(singleEvent(parse(server.requests.poll().body())).get("properties"));
        assertEquals("company", eventProperties.get("$group_type"));
        assertEquals("acme", eventProperties.get("$group_key"));
        assertFalse(eventProperties.containsKey("$group_set"), "$group_set must be omitted when there are no properties");
    }

    @Test
    void group_withEmptyPropertiesOmitsGroupSet() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        group("user-1", "company", "acme", properties("{}")).run(runContext);

        Map<String, Object> eventProperties = map(singleEvent(parse(server.requests.poll().body())).get("properties"));
        assertEquals("acme", eventProperties.get("$group_key"));
        assertFalse(eventProperties.containsKey("$group_set"), "$group_set must be omitted when the properties are empty");
    }

    @Test
    void group_omitsNullPropertyValues() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        group("user-1", "company", "acme", properties("{\"name\": \"Acme\", \"cleared\": null}")).run(runContext);

        Map<String, Object> groupSet = groupSet(singleEvent(parse(server.requests.poll().body())));
        assertEquals("Acme", groupSet.get("name"));
        assertFalse(groupSet.containsKey("cleared"), "a null value must not appear in $group_set");
    }

    @Test
    void group_trimsWhitespace() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());

        group("  user-1  ", "\tcompany ", " acme\n", null).run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals("user-1", event.get("distinct_id"));
        assertEquals("company", map(event.get("properties")).get("$group_type"));
        assertEquals("acme", map(event.get("properties")).get("$group_key"));
    }

    @Test
    void group_rejectsMissingAndBlankDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String invalid : new String[]{null, "", "   "}) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> group(invalid, "company", "acme", null).run(runContext));
            assertTrue(exception.getMessage().contains("`distinctId`"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing or blank distinctId");
    }

    @Test
    void group_rejectsMissingAndBlankGroupType() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String invalid : new String[]{null, "", "   "}) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> group("user-1", invalid, "acme", null).run(runContext));
            assertTrue(exception.getMessage().contains("`groupType`"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing or blank groupType");
    }

    @Test
    void group_rejectsMissingAndBlankGroupKey() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String invalid : new String[]{null, "", "   "}) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> group("user-1", "company", invalid, null).run(runContext));
            assertTrue(exception.getMessage().contains("`groupKey`"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing or blank groupKey");
    }

    @Test
    void group_rejectsBlankPropertyName() {
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalArgumentException emptyName = assertThrows(IllegalArgumentException.class,
            () -> group("user-1", "company", "acme", properties("{\"\": \"something\", \"name\": \"Acme\"}")).run(runContext));
        IllegalArgumentException spacesName = assertThrows(IllegalArgumentException.class,
            () -> group("user-1", "company", "acme", properties("{\"   \": \"something\"}")).run(runContext));

        assertTrue(emptyName.getMessage().contains("blank property name"), emptyName.getMessage());
        assertTrue(spacesName.getMessage().contains("blank property name"), spacesName.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent for a blank property name");
    }

    @Test
    void group_preservesSpecialCharactersAndUtf8() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of());
        String groupType = "org/team & \"co\"";
        String groupKey = "id+1@example.com é日本語 😀";

        group("user+tag@example.com", groupType, groupKey, properties("{\"na\\u00efve name\": \"v\\u65e5\"}")).run(runContext);

        Map<String, Object> event = singleEvent(parse(server.requests.poll().body()));
        assertEquals("user+tag@example.com", event.get("distinct_id"));
        assertEquals(groupType, map(event.get("properties")).get("$group_type"));
        assertEquals(groupKey, map(event.get("properties")).get("$group_key"));
        assertEquals("v日", groupSet(event).get("naïve name"));
    }

    @Test
    void group_waitsForFlushBeforeReturning() throws Exception {
        server.responseDelayMillis = 700;
        RunContext runContext = runContextFactory.of(Map.of());

        long start = System.nanoTime();
        group("user-1", "company", "acme", null).run(runContext);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // the flag is set right before the server sends its response, so if run() has returned,
        // it can only have done so after the HTTP exchange was completed
        assertTrue(server.responseSent.get(), "run() returned before the server sent its response");
        assertTrue(elapsedMillis >= 600, "run() returned after only " + elapsedMillis + "ms, it did not wait for the delayed response");
        assertNotNull(server.requests.poll(), "no request reached the server before run() returned");
    }

    @Test
    void group_rejectsBlankApiKey() {
        RunContext runContext = runContextFactory.of(Map.of());

        Group group = Group.builder()
            .id("group")
            .type(Group.class.getName())
            .apiKey(Property.ofValue("   "))
            .host(Property.ofValue(server.url()))
            .distinctId(Property.ofValue("user-1"))
            .groupType(Property.ofValue("company"))
            .groupKey(Property.ofValue("acme"))
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> group.run(runContext));

        assertTrue(exception.getMessage().contains("apiKey"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent without an API key");
    }

    private Group group(String distinctId, String groupType, String groupKey, Property<Map<String, Object>> groupProperties) {
        return group(value(distinctId), value(groupType), value(groupKey), groupProperties);
    }

    private Group group(Property<String> distinctId, Property<String> groupType, Property<String> groupKey, Property<Map<String, Object>> groupProperties) {
        return Group.builder()
            .id("group")
            .type(Group.class.getName())
            .apiKey(Property.ofValue(API_KEY))
            .host(Property.ofValue(server.url()))
            .distinctId(distinctId)
            .groupType(groupType)
            .groupKey(groupKey)
            .groupProperties(groupProperties)
            .build();
    }

    private static Property<String> value(String value) {
        return value == null ? null : Property.ofValue(value);
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

    private static Map<String, Object> groupSet(Map<String, Object> event) {
        return map(map(event.get("properties")).get("$group_set"));
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
