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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class EvaluateFeatureFlagTest {
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
    void evaluate_enabledBooleanFlag() throws Exception {
        server.flagsBody = flagsResponse(flag("on_flag", true, null));
        RunContext runContext = runContextFactory.of(Map.of());

        EvaluateFeatureFlag.Output output = evaluate("user-1", "on_flag").run(runContext);

        assertEquals(Boolean.TRUE, output.getValue());
        assertEquals(Boolean.TRUE, output.getEnabled());
        assertNull(output.getVariant());

        List<ReceivedRequest> flagsRequests = server.flagsRequests();
        assertEquals(1, flagsRequests.size());
        ReceivedRequest request = flagsRequests.getFirst();
        assertEquals("POST", request.method());
        assertEquals("/flags/", request.path());
        assertEquals("v=2", request.query());
        assertNull(request.authorization(), "the project key goes in the body, not in an Authorization header");

        Map<String, Object> body = parse(request.body());
        assertEquals(API_KEY, body.get("api_key"));
        assertEquals("user-1", body.get("distinct_id"));
        assertEquals(List.of("on_flag"), body.get("flag_keys_to_evaluate"));
    }

    @Test
    void evaluate_disabledBooleanFlag() throws Exception {
        server.flagsBody = flagsResponse(flag("off_flag", false, null));
        RunContext runContext = runContextFactory.of(Map.of());

        EvaluateFeatureFlag.Output output = evaluate("user-1", "off_flag").run(runContext);

        assertEquals(Boolean.FALSE, output.getValue());
        assertEquals(Boolean.FALSE, output.getEnabled());
        assertNull(output.getVariant());
    }

    @Test
    void evaluate_multivariateFlag() throws Exception {
        server.flagsBody = flagsResponse(flag("multi_flag", true, "test-b"));
        RunContext runContext = runContextFactory.of(Map.of());

        EvaluateFeatureFlag.Output output = evaluate("user-1", "multi_flag").run(runContext);

        assertEquals("test-b", output.getValue());
        assertEquals(Boolean.TRUE, output.getEnabled());
        assertEquals("test-b", output.getVariant());
    }

    @Test
    void evaluate_missingFlagIsNotAnError() throws Exception {
        // the evaluation itself succeeds (there is a request id), but PostHog does not return the requested flag
        server.flagsBody = flagsResponse(flag("some_other_flag", true, null));
        RunContext runContext = runContextFactory.of(Map.of());

        EvaluateFeatureFlag.Output output = evaluate("user-1", "missing_flag").run(runContext);

        assertNull(output.getValue());
        assertEquals(Boolean.FALSE, output.getEnabled());
        assertNull(output.getVariant());
        assertEquals(1, server.flagsRequests().size());
    }

    @Test
    void evaluate_rendersTemplatedDistinctIdAndFlagKey() throws Exception {
        server.flagsBody = flagsResponse(flag("new-checkout", true, null));
        RunContext runContext = runContextFactory.of(Map.of("user_id", "user-42", "flag", "new-checkout"));

        // Property.ofValue() never renders, so build the properties from JSON like Kestra does when it reads a flow
        EvaluateFeatureFlag.Output output = evaluate(text("{{ user_id }}"), text("{{ flag }}")).run(runContext);

        Map<String, Object> body = parse(server.flagsRequests().getFirst().body());
        assertEquals("user-42", body.get("distinct_id"));
        assertEquals(List.of("new-checkout"), body.get("flag_keys_to_evaluate"));
        assertEquals(Boolean.TRUE, output.getValue());
    }

    @Test
    void evaluate_trimsWhitespace() throws Exception {
        server.flagsBody = flagsResponse(flag("on_flag", true, null));
        RunContext runContext = runContextFactory.of(Map.of());

        evaluate("  user-1  ", " on_flag\t").run(runContext);

        Map<String, Object> body = parse(server.flagsRequests().getFirst().body());
        assertEquals("user-1", body.get("distinct_id"));
        assertEquals(List.of("on_flag"), body.get("flag_keys_to_evaluate"));
    }

    @Test
    void evaluate_rejectsMissingAndBlankDistinctId() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String invalid : new String[]{null, "", "   "}) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> evaluate(invalid, "on_flag").run(runContext));
            assertTrue(exception.getMessage().contains("`distinctId`"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing or blank distinctId");
    }

    @Test
    void evaluate_rejectsMissingAndBlankFlagKey() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String invalid : new String[]{null, "", "   "}) {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> evaluate("user-1", invalid).run(runContext));
            assertTrue(exception.getMessage().contains("`flagKey`"), exception.getMessage());
        }
        assertTrue(server.requests.isEmpty(), "no request should be sent for a missing or blank flagKey");
    }

    @Test
    void evaluate_failsOnHttpErrorsInsteadOfReturningDisabled() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (int status : new int[]{500, 401, 404, 429}) {
            server.flagsStatus = status;
            server.flagsBody = "{\"detail\":\"error\"}";

            IllegalStateException exception = assertThrows(IllegalStateException.class, () -> evaluate("user-1", "on_flag").run(runContext), "HTTP " + status);
            assertTrue(exception.getMessage().contains("evaluation failed"), exception.getMessage());
        }
        assertTrue(server.batchRequests().isEmpty(), "no $feature_flag_called event should be sent for a failed evaluation");
    }

    @Test
    void evaluate_failsOnMalformedAndEmptyResponses() {
        RunContext runContext = runContextFactory.of(Map.of());

        for (String body : new String[]{"{not json", "", "{}"}) {
            server.flagsBody = body;

            IllegalStateException exception = assertThrows(IllegalStateException.class, () -> evaluate("user-1", "on_flag").run(runContext), "body '" + body + "'");
            assertTrue(exception.getMessage().contains("evaluation failed"), exception.getMessage());
        }
    }

    @Test
    void evaluate_failsWhenServerIsUnreachable() {
        RunContext runContext = runContextFactory.of(Map.of());

        EvaluateFeatureFlag unreachable = EvaluateFeatureFlag.builder()
            .id("evaluate")
            .type(EvaluateFeatureFlag.class.getName())
            .apiKey(Property.ofValue(API_KEY))
            .host(Property.ofValue("http://127.0.0.1:1"))
            .distinctId(Property.ofValue("user-1"))
            .flagKey(Property.ofValue("on_flag"))
            .build();

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> unreachable.run(runContext));

        assertTrue(exception.getMessage().contains("evaluation failed"), exception.getMessage());
    }

    @Test
    void evaluate_retriesOnceOnBadGatewayThenFails() {
        server.flagsStatus = 502;
        server.flagsBody = "{}";
        RunContext runContext = runContextFactory.of(Map.of());

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> evaluate("user-1", "on_flag").run(runContext));

        assertTrue(exception.getMessage().contains("evaluation failed"), exception.getMessage());
        assertEquals(2, server.flagsRequests().size(), "the SDK retries a 502 once by default");
    }

    @Test
    void evaluate_sendsFeatureFlagCalledEventAndWaitsForFlush() throws Exception {
        server.flagsBody = flagsResponse(flag("multi_flag", true, "test-b"));
        server.batchDelayMillis = 700;
        RunContext runContext = runContextFactory.of(Map.of());

        long start = System.nanoTime();
        evaluate("user-1", "multi_flag").run(runContext);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        // the flag is set right before the server sends its /batch response, so if run() has returned,
        // it can only have done so after the HTTP exchange was completed
        assertTrue(server.batchResponseSent.get(), "run() returned before the server sent its /batch response");
        assertTrue(elapsedMillis >= 600, "run() returned after only " + elapsedMillis + "ms, it did not wait for the delayed response");

        List<ReceivedRequest> batchRequests = server.batchRequests();
        assertEquals(1, batchRequests.size());
        Map<String, Object> body = parse(batchRequests.getFirst().body());
        assertEquals(API_KEY, body.get("api_key"));

        List<Map<String, Object>> events = events(body);
        assertEquals(1, events.size(), "reading the flag once must queue a single event");
        Map<String, Object> event = events.getFirst();
        assertEquals("$feature_flag_called", event.get("event"));
        assertEquals("user-1", event.get("distinct_id"));
        Map<String, Object> properties = map(event.get("properties"));
        assertEquals("multi_flag", properties.get("$feature_flag"));
        assertEquals("test-b", properties.get("$feature_flag_response"));
        assertEquals("req-123", properties.get("$feature_flag_request_id"));
    }

    @Test
    void evaluate_rejectsBlankApiKey() {
        RunContext runContext = runContextFactory.of(Map.of());

        EvaluateFeatureFlag evaluate = EvaluateFeatureFlag.builder()
            .id("evaluate")
            .type(EvaluateFeatureFlag.class.getName())
            .apiKey(Property.ofValue("   "))
            .host(Property.ofValue(server.url()))
            .distinctId(Property.ofValue("user-1"))
            .flagKey(Property.ofValue("on_flag"))
            .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> evaluate.run(runContext));

        assertTrue(exception.getMessage().contains("apiKey"), exception.getMessage());
        assertTrue(server.requests.isEmpty(), "no request should be sent without an API key");
    }

    private EvaluateFeatureFlag evaluate(String distinctId, String flagKey) {
        return evaluate(
            distinctId == null ? null : Property.ofValue(distinctId),
            flagKey == null ? null : Property.ofValue(flagKey)
        );
    }

    private EvaluateFeatureFlag evaluate(Property<String> distinctId, Property<String> flagKey) {
        return EvaluateFeatureFlag.builder()
            .id("evaluate")
            .type(EvaluateFeatureFlag.class.getName())
            .apiKey(Property.ofValue(API_KEY))
            .host(Property.ofValue(server.url()))
            .distinctId(distinctId)
            .flagKey(flagKey)
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

    private static String flag(String key, boolean enabled, String variant) {
        return "\"" + key + "\":{\"key\":\"" + key + "\",\"enabled\":" + enabled + ",\"variant\":" + (variant == null ? "null" : "\"" + variant + "\"")
            + ",\"reason\":{\"code\":\"condition_match\",\"description\":\"Matched condition set 1\",\"condition_index\":0}"
            + ",\"metadata\":{\"id\":1,\"version\":1,\"payload\":null}}";
    }

    private static String flagsResponse(String... flags) {
        return "{\"flags\":{" + String.join(",", flags) + "},\"errorsWhileComputingFlags\":false,\"requestId\":\"req-123\",\"evaluatedAt\":1700000000000}";
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
    private static List<Map<String, Object>> events(Map<String, Object> body) {
        return (List<Map<String, Object>>) body.get("batch");
    }

    private record ReceivedRequest(String method, String path, String query, String authorization, String body) {
    }

    /**
     * Minimal local stand-in for the PostHog API: answers {@code /flags/} with a configurable status and body,
     * and any other path (i.e. {@code /batch}) with {@code 200}. Records every request (gzip bodies are decompressed).
     */
    private static class MockPosthogServer {
        final List<ReceivedRequest> requests = new CopyOnWriteArrayList<>();
        final AtomicBoolean batchResponseSent = new AtomicBoolean(false);
        volatile int flagsStatus = 200;
        volatile String flagsBody = "{}";
        volatile long batchDelayMillis = 0;

        private final HttpServer httpServer;

        MockPosthogServer() throws Exception {
            httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            httpServer.createContext("/", exchange -> {
                byte[] raw = exchange.getRequestBody().readAllBytes();
                String encoding = exchange.getRequestHeaders().getFirst("Content-Encoding");
                byte[] decoded = encoding != null && encoding.contains("gzip")
                    ? new GZIPInputStream(new ByteArrayInputStream(raw)).readAllBytes()
                    : raw;

                String path = exchange.getRequestURI().getPath();
                requests.add(new ReceivedRequest(
                    exchange.getRequestMethod(),
                    path,
                    exchange.getRequestURI().getQuery(),
                    exchange.getRequestHeaders().getFirst("Authorization"),
                    new String(decoded, StandardCharsets.UTF_8)
                ));

                boolean flags = path.startsWith("/flags");
                if (!flags && batchDelayMillis > 0) {
                    try {
                        Thread.sleep(batchDelayMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                byte[] response = (flags ? flagsBody : "{\"status\":\"Ok\"}").getBytes(StandardCharsets.UTF_8);
                if (!flags) {
                    batchResponseSent.set(true);
                }
                if (response.length == 0) {
                    exchange.sendResponseHeaders(flags ? flagsStatus : 200, -1);
                } else {
                    exchange.sendResponseHeaders(flags ? flagsStatus : 200, response.length);
                    exchange.getResponseBody().write(response);
                }
                exchange.close();
            });
            httpServer.start();
        }

        List<ReceivedRequest> flagsRequests() {
            return new ArrayList<>(requests.stream().filter(request -> request.path().startsWith("/flags")).toList());
        }

        List<ReceivedRequest> batchRequests() {
            return new ArrayList<>(requests.stream().filter(request -> request.path().startsWith("/batch")).toList());
        }

        String url() {
            return "http://127.0.0.1:" + httpServer.getAddress().getPort();
        }

        void stop() {
            httpServer.stop(0);
        }
    }
}
