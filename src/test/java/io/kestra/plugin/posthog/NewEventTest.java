package io.kestra.plugin.posthog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpServer;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@KestraTest
class NewEventTest {
    private static final String API_KEY = "phx_secret_test_key";
    private static final String MIN_UUID = "00000000-0000-0000-0000-000000000000";
    private static final Instant BASE = Instant.parse("2026-01-01T10:00:00Z");
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    @Inject
    private RunContextFactory runContextFactory;

    private MockPosthogApi api;

    @BeforeEach
    void startServer() throws Exception {
        api = new MockPosthogApi();
    }

    @AfterEach
    void stopServer() {
        api.stop();
    }

    // ---------------------------------------------------------------- cursor and polling

    @Test
    void firstPoll_initializesCursorAndReturnsNoExecution() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        Poll poll = poll(trigger().build());

        Instant before = Instant.now();
        Optional<Execution> result = poll.run();
        Instant after = Instant.now();

        assertTrue(result.isEmpty());
        assertTrue(api.requests.isEmpty(), "the first poll must not query PostHog nor replay history");
        Map<String, Object> cursor = cursor(poll);
        Instant cursorTimestamp = Instant.parse((String) cursor.get("timestamp"));
        assertFalse(cursorTimestamp.isBefore(before.minusMillis(1)) || cursorTimestamp.isAfter(after.plusMillis(1)), "the cursor must be the current time");
        assertEquals(MIN_UUID, cursor.get("uuid"));
    }

    @Test
    void subsequentPoll_returnsNewEventsWithMappedFieldsAndCount() throws Exception {
        api.add("00000000-0000-0000-0000-000000000001", "signup", "user-1", BASE.plusSeconds(1), "{\"plan\":\"pro\",\"nested\":{\"n\":2},\"count\":3}");
        api.add("00000000-0000-0000-0000-000000000002", "purchase", "user-2", BASE.plusSeconds(2), "{}");
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        Execution execution = poll.run().orElseThrow();

        Map<String, Object> variables = execution.getTrigger().getVariables();
        assertEquals(2, variables.get("count"));
        List<Map<String, Object>> events = events(execution);
        assertEquals(2, events.size());
        assertEquals("00000000-0000-0000-0000-000000000001", events.get(0).get("id"));
        assertEquals("signup", events.get(0).get("event"));
        assertEquals("user-1", events.get(0).get("distinctId"));
        assertEquals(BASE.plusSeconds(1), timestamp(events.get(0)));
        Map<String, Object> properties = map(events.get(0).get("properties"));
        assertEquals("pro", properties.get("plan"));
        assertEquals(3, properties.get("count"));
        assertEquals(2, map(properties.get("nested")).get("n"));
        assertEquals("purchase", events.get(1).get("event"));
        assertEquals(BASE.plusSeconds(2), timestamp(events.get(1)));
    }

    @Test
    void events_areNotRepeatedAfterTheCursorAdvanced() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        api.add("e2", "signup", BASE.plusSeconds(2));
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        assertEquals(2, events(poll.run().orElseThrow()).size());
        assertTrue(poll.run().isEmpty(), "the same events must not be returned twice");

        api.add("e3", "signup", BASE.plusSeconds(3));
        List<Map<String, Object>> next = events(poll.run().orElseThrow());
        assertEquals(List.of("e3"), next.stream().map(e -> e.get("id")).toList());
        assertEquals(3, api.requests.size());
    }

    @Test
    void cursor_isPersistedOnlyWithTheLastReturnedEvent() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        api.add("e2", "signup", BASE.plusSeconds(2));
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        poll.run().orElseThrow();

        Map<String, Object> cursor = cursor(poll);
        assertEquals(BASE.plusSeconds(2), Instant.parse((String) cursor.get("timestamp")));
        assertEquals("e2", cursor.get("uuid"));
    }

    @Test
    void sameTimestamp_isSeparatedByUuidAndNothingIsRepeatedOrSkipped() throws Exception {
        Instant same = BASE.plusSeconds(5);
        api.add("0000000a", "signup", same);
        api.add("0000000b", "signup", same);
        api.add("0000000c", "signup", same);
        Poll poll = poll(trigger().maxEvents(Property.ofValue(1)).build());
        seed(poll, BASE, MIN_UUID);

        List<Object> seen = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            seen.addAll(events(poll.run().orElseThrow()).stream().map(e -> e.get("id")).toList());
        }

        assertEquals(List.of("0000000a", "0000000b", "0000000c"), seen);
        assertTrue(poll.run().isEmpty());
    }

    @Test
    void maxEvents_one_returnsOneEventPerPoll() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        api.add("e2", "signup", BASE.plusSeconds(2));
        Poll poll = poll(trigger().maxEvents(Property.ofValue(1)).build());
        seed(poll, BASE, MIN_UUID);

        Execution first = poll.run().orElseThrow();

        assertEquals(1, first.getTrigger().getVariables().get("count"));
        assertEquals("e1", events(first).getFirst().get("id"));
        assertTrue(api.lastRequest().query().endsWith("LIMIT 1"));
    }

    @Test
    void maxEvents_exactlyTheNumberOfNewEvents_returnsAllOfThem() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        api.add("e2", "signup", BASE.plusSeconds(2));
        api.add("e3", "signup", BASE.plusSeconds(3));
        Poll poll = poll(trigger().maxEvents(Property.ofValue(3)).build());
        seed(poll, BASE, MIN_UUID);

        assertEquals(3, events(poll.run().orElseThrow()).size());
        assertTrue(poll.run().isEmpty());
    }

    @Test
    void backlogLargerThanMaxEvents_drainsOverSuccessivePolls() throws Exception {
        for (int i = 1; i <= 5; i++) {
            api.add("e" + i, "signup", BASE.plusSeconds(i));
        }
        Poll poll = poll(trigger().maxEvents(Property.ofValue(2)).build());
        seed(poll, BASE, MIN_UUID);

        List<List<Object>> polls = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            polls.add(events(poll.run().orElseThrow()).stream().map(e -> e.get("id")).toList());
        }

        assertEquals(List.of(List.of("e1", "e2"), List.of("e3", "e4"), List.of("e5")), polls);
        assertTrue(poll.run().isEmpty());
    }

    @Test
    void maxEvents_atTheUpperBound_isAccepted() throws Exception {
        Poll poll = poll(trigger().maxEvents(Property.ofValue(50_000)).build());
        seed(poll, BASE, MIN_UUID);

        assertTrue(poll.run().isEmpty());

        assertTrue(api.lastRequest().query().endsWith("LIMIT 50000"));
    }

    @Test
    void maxEvents_outOfRange_isRejectedBeforeAnyRequest() throws Exception {
        for (int invalid : new int[]{0, -1, 50_001}) {
            Poll poll = poll(trigger().maxEvents(Property.ofValue(invalid)).build());
            seed(poll, BASE, MIN_UUID);

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, poll::run, "maxEvents " + invalid);
            assertTrue(exception.getMessage().contains("`maxEvents`"), exception.getMessage());
        }
        assertTrue(api.requests.isEmpty());
    }

    @Test
    void zeroNewEvents_returnsNoExecutionAndKeepsTheCursor() throws Exception {
        Poll poll = poll(trigger().build());
        seed(poll, BASE, "cursor-uuid");

        assertTrue(poll.run().isEmpty());

        assertEquals(1, api.requests.size());
        assertEquals("cursor-uuid", cursor(poll).get("uuid"));
        assertEquals(BASE, Instant.parse((String) cursor(poll).get("timestamp")));
    }

    // ---------------------------------------------------------------- request

    @Test
    void request_usesTheProjectUrlBearerAuthenticationAndTheDocumentedBody() throws Exception {
        Poll poll = poll(trigger().projectId(Property.ofValue(4242L)).build());
        seed(poll, Instant.parse("2026-01-01T10:00:00.123456Z"), "cursor-uuid");

        poll.run();

        MockPosthogApi.Received request = api.lastRequest();
        assertEquals("POST", request.method());
        assertEquals("/api/projects/4242/query/", request.path());
        assertEquals("Bearer " + API_KEY, request.headers().get("authorization"));
        assertTrue(request.headers().get("content-type").startsWith("application/json"), request.headers().get("content-type"));

        assertEquals("force_blocking", request.body().get("refresh"));
        Map<String, Object> query = map(request.body().get("query"));
        assertEquals("HogQLQuery", query.get("kind"));
        assertEquals(Map.of("convertToProjectTimezone", false), query.get("modifiers"));
        Map<String, Object> values = map(query.get("values"));
        assertEquals("2026-01-01 10:00:00.123456", values.get("cursorTs"));
        assertEquals("cursor-uuid", values.get("cursorUuid"));
        assertFalse(values.containsKey("eventName"));

        String sql = (String) query.get("query");
        assertEquals("SELECT uuid, event, distinct_id, timestamp, properties FROM events "
            + "WHERE timestamp >= {cursorTs} AND (timestamp > {cursorTs} OR uuid > {cursorUuid}) "
            + "ORDER BY timestamp ASC, uuid ASC LIMIT 100", sql);
        assertFalse(sql.contains("cursor-uuid"), "the cursor must be a bound value, not part of the query text");
    }

    @Test
    void eventName_isABoundValueAndFiltersTheEvents() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        api.add("e2", "purchase", BASE.plusSeconds(2));
        api.add("e3", "signup", BASE.plusSeconds(3));
        Poll poll = poll(trigger().eventName(Property.ofValue("signup")).build());
        seed(poll, BASE, MIN_UUID);

        List<Map<String, Object>> events = events(poll.run().orElseThrow());

        assertEquals(List.of("e1", "e3"), events.stream().map(e -> e.get("id")).toList());
        Map<String, Object> query = map(api.lastRequest().body().get("query"));
        assertEquals("signup", map(query.get("values")).get("eventName"));
        assertTrue(((String) query.get("query")).contains(" AND event = {eventName} "));
    }

    @Test
    void eventName_isNeverConcatenatedInTheQueryText() throws Exception {
        String malicious = "x' OR 1=1 --";
        Poll poll = poll(trigger().eventName(Property.ofValue(malicious)).build());
        seed(poll, BASE, MIN_UUID);

        poll.run();

        Map<String, Object> query = map(api.lastRequest().body().get("query"));
        assertEquals(malicious, map(query.get("values")).get("eventName"));
        assertFalse(((String) query.get("query")).contains("OR 1=1"));
    }

    @Test
    void withoutEventName_theQueryHasNoEventFilter() throws Exception {
        Poll poll = poll(trigger().eventName(Property.ofValue("   ")).build());
        seed(poll, BASE, MIN_UUID);

        poll.run();

        Map<String, Object> query = map(api.lastRequest().body().get("query"));
        assertFalse(((String) query.get("query")).contains("event = "));
        assertFalse(map(query.get("values")).containsKey("eventName"));
    }

    @Test
    void templatedInputs_areRendered() throws Exception {
        api.add("e1", "user_signed_up", BASE.plusSeconds(1));
        NewEvent trigger = trigger()
            .personalApiKey(templated("{{ 'phx_' ~ 'templated' }}", new TypeReference<>() {
            }))
            .projectId(templated("{{ 6 * 7 }}", new TypeReference<>() {
            }))
            .host(templated("{{ 'http://127.0.0.1:' ~ '" + api.port() + "' }}", new TypeReference<>() {
            }))
            .eventName(templated("{{ 'user_' ~ 'signed_up' }}", new TypeReference<>() {
            }))
            .maxEvents(templated("{{ 2 * 3 }}", new TypeReference<>() {
            }))
            .build();
        Poll poll = poll(trigger);
        seed(poll, BASE, MIN_UUID);

        Execution execution = poll.run().orElseThrow();

        MockPosthogApi.Received request = api.lastRequest();
        assertEquals("/api/projects/42/query/", request.path());
        assertEquals("Bearer phx_templated", request.headers().get("authorization"));
        assertTrue(request.query().endsWith("LIMIT 6"));
        assertEquals("user_signed_up", map(map(request.body().get("query")).get("values")).get("eventName"));
        assertEquals(1, events(execution).size());
    }

    // ---------------------------------------------------------------- hosts

    @Test
    void queryUri_supportsUsEuAndSelfHostedHosts() {
        assertEquals("https://us.posthog.com/api/projects/123/query/", NewEvent.queryUri("https://us.posthog.com", 123).toString());
        assertEquals("https://eu.posthog.com/api/projects/123/query/", NewEvent.queryUri("https://eu.posthog.com", 123).toString());
        assertEquals("https://posthog.mycompany.io:8443/api/projects/7/query/", NewEvent.queryUri("https://posthog.mycompany.io:8443", 7).toString());
        assertEquals("http://localhost:8000/analytics/api/projects/7/query/", NewEvent.queryUri("http://localhost:8000/analytics", 7).toString());
    }

    @Test
    void queryUri_removesTrailingSlashes() {
        assertEquals("https://eu.posthog.com/api/projects/123/query/", NewEvent.queryUri("https://eu.posthog.com/", 123).toString());
        assertEquals("https://eu.posthog.com/api/projects/123/query/", NewEvent.queryUri("  https://eu.posthog.com///  ", 123).toString());
    }

    @Test
    void defaults_areTheUsAppHostFiveMinutesAndOneHundredEvents() throws Exception {
        NewEvent trigger = NewEvent.builder()
            .id("defaults")
            .type(NewEvent.class.getName())
            .personalApiKey(Property.ofValue(API_KEY))
            .projectId(Property.ofValue(1L))
            .build();
        Poll poll = poll(trigger);

        assertEquals("https://us.posthog.com", poll.conditionContext.getRunContext().render(trigger.getHost()).as(String.class).orElseThrow());
        assertEquals(100, poll.conditionContext.getRunContext().render(trigger.getMaxEvents()).as(Integer.class).orElseThrow());
        assertEquals(Duration.ofMinutes(5), trigger.getInterval());
    }

    @Test
    void selfHostedHostWithPathAndTrailingSlash_isUsedForTheRequest() throws Exception {
        Poll poll = poll(trigger().host(Property.ofValue(api.url() + "/posthog/")).build());
        seed(poll, BASE, MIN_UUID);

        poll.run();

        assertEquals("/posthog/api/projects/123/query/", api.lastRequest().path());
    }

    // ---------------------------------------------------------------- invalid configuration

    @Test
    void invalidProjectId_isRejected() throws Exception {
        for (long invalid : new long[]{0, -3}) {
            Poll poll = poll(trigger().projectId(Property.ofValue(invalid)).build());

            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, poll::run, "projectId " + invalid);
            assertTrue(exception.getMessage().contains("`projectId`"), exception.getMessage());
        }
        assertTrue(api.requests.isEmpty());
    }

    @Test
    void blankPersonalApiKey_isRejected() throws Exception {
        Poll poll = poll(trigger().personalApiKey(Property.ofValue("   ")).build());

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, poll::run);

        assertTrue(exception.getMessage().contains("`personalApiKey`"), exception.getMessage());
        assertTrue(api.requests.isEmpty());
    }

    // ---------------------------------------------------------------- at-least-once

    @Test
    void cursor_isNotAdvancedWhenTheExecutionCannotBeCreated() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        api.add("e2", "signup", BASE.plusSeconds(2));
        NewEvent trigger = trigger().build();
        Poll poll = poll(trigger);
        seed(poll, BASE, MIN_UUID);

        // without a flow, the execution cannot be generated
        ConditionContext withoutFlow = ConditionContext.builder().runContext(poll.conditionContext.getRunContext()).build();
        assertThrows(Exception.class, () -> trigger.evaluate(withoutFlow, poll.triggerContext));

        assertEquals(BASE, Instant.parse((String) cursor(poll).get("timestamp")), "the cursor must not move when the execution failed");
        assertEquals(MIN_UUID, cursor(poll).get("uuid"));

        // so that the same events are returned again by the next poll
        assertEquals(List.of("e1", "e2"), events(poll.run().orElseThrow()).stream().map(e -> e.get("id")).toList());
        assertEquals("e2", cursor(poll).get("uuid"));
    }

    // ---------------------------------------------------------------- errors

    @Test
    void http400_failsWithTheResponseDetailAndKeepsTheCursor() throws Exception {
        api.reply = new MockPosthogApi.Reply(400, "{\"type\":\"validation_error\",\"detail\":\"Unable to parse the HogQL query\"}", Map.of());
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        IllegalStateException exception = assertThrows(IllegalStateException.class, poll::run);

        assertTrue(exception.getMessage().contains("HTTP 400"), exception.getMessage());
        assertTrue(exception.getMessage().contains("Unable to parse the HogQL query"), exception.getMessage());
        assertEquals(MIN_UUID, cursor(poll).get("uuid"));
    }

    @Test
    void http401_403_404_failWithAClearMessageWithoutTheApiKey() throws Exception {
        Object[][] cases = {
            {401, "HTTP 401", "personalApiKey"},
            {403, "HTTP 403", "Query Read"},
            {404, "HTTP 404", "projectId"},
        };
        for (Object[] failure : cases) {
            api.reply = new MockPosthogApi.Reply((int) failure[0], "{\"detail\":\"nope\"}", Map.of());
            Poll poll = poll(trigger().build());
            seed(poll, BASE, MIN_UUID);

            IllegalStateException exception = assertThrows(IllegalStateException.class, poll::run, String.valueOf(failure[0]));

            assertTrue(exception.getMessage().contains((String) failure[1]), exception.getMessage());
            assertTrue(exception.getMessage().contains((String) failure[2]), exception.getMessage());
            assertTrue(exception.getMessage().contains("nope"), exception.getMessage());
            assertFalse(exception.getMessage().contains(API_KEY), "the API key must never be part of an error message");
            assertEquals(MIN_UUID, cursor(poll).get("uuid"));
        }
    }

    @Test
    void http429_isSkippedAndRetriedOnTheNextPoll() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        api.reply = new MockPosthogApi.Reply(429, "{\"code\":\"api_queries_budget_exceeded\"}", Map.of("Retry-After", "30"));
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        assertTrue(poll.run().isEmpty(), "a rate limit must not fail the trigger");
        assertEquals(MIN_UUID, cursor(poll).get("uuid"));

        api.reply = null;
        assertEquals(List.of("e1"), events(poll.run().orElseThrow()).stream().map(e -> e.get("id")).toList());
    }

    @Test
    void serverErrors_areSkippedAndRetriedOnTheNextPoll() throws Exception {
        api.add("e1", "signup", BASE.plusSeconds(1));
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        for (int status : new int[]{500, 502, 503}) {
            api.reply = new MockPosthogApi.Reply(status, "{\"detail\":\"down\"}", Map.of());

            assertTrue(poll.run().isEmpty(), "HTTP " + status);
            assertEquals(MIN_UUID, cursor(poll).get("uuid"));
        }

        api.reply = null;
        assertEquals(1, events(poll.run().orElseThrow()).size());
    }

    @Test
    void unreachableServer_isSkippedAndKeepsTheCursor() throws Exception {
        Poll poll = poll(trigger().host(Property.ofValue("http://127.0.0.1:1")).build());
        seed(poll, BASE, MIN_UUID);

        assertTrue(poll.run().isEmpty());

        assertEquals(MIN_UUID, cursor(poll).get("uuid"));
    }

    @Test
    void malformedJson_failsAndKeepsTheCursor() throws Exception {
        api.reply = new MockPosthogApi.Reply(200, "{not json", Map.of());
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        IllegalStateException exception = assertThrows(IllegalStateException.class, poll::run);

        assertTrue(exception.getMessage().contains("not valid JSON"), exception.getMessage());
        assertEquals(MIN_UUID, cursor(poll).get("uuid"));
    }

    @Test
    void unexpectedResponseShapes_failAndKeepTheCursor() throws Exception {
        String[] bodies = {
            "{}",
            "{\"results\":\"nope\"}",
            "{\"results\":[[\"a\"]]}",
            "{\"columns\":[\"event\",\"distinct_id\",\"timestamp\",\"properties\"],\"results\":[[\"signup\",\"u\",\"2026-01-01T10:00:01Z\",\"{}\"]]}",
            "{\"columns\":[\"uuid\",\"event\",\"distinct_id\",\"timestamp\",\"properties\"],\"results\":[[\"e1\",\"signup\"]]}",
            "{\"columns\":[\"uuid\",\"event\",\"distinct_id\",\"timestamp\",\"properties\"],\"results\":[[null,\"signup\",\"u\",\"2026-01-01T10:00:01Z\",\"{}\"]]}",
            "{\"columns\":[\"uuid\",\"event\",\"distinct_id\",\"timestamp\",\"properties\"],\"results\":[[\"e1\",\"signup\",\"u\",12345,\"{}\"]]}",
            "{\"columns\":[\"uuid\",\"event\",\"distinct_id\",\"timestamp\",\"properties\"],\"results\":[[\"e1\",\"signup\",\"u\",\"yesterday\",\"{}\"]]}",
            "{\"columns\":[\"uuid\",\"event\",\"distinct_id\",\"timestamp\",\"properties\"],\"results\":[[\"e1\",\"signup\",\"u\",\"2026-01-01T10:00:01Z\",\"[1,2]\"]]}",
        };
        for (String body : bodies) {
            api.reply = new MockPosthogApi.Reply(200, body, Map.of());
            Poll poll = poll(trigger().build());
            seed(poll, BASE, MIN_UUID);

            IllegalStateException exception = assertThrows(IllegalStateException.class, poll::run, body);

            assertTrue(exception.getMessage().contains("PostHog returned"), exception.getMessage());
            assertEquals(MIN_UUID, cursor(poll).get("uuid"), body);
        }
    }

    // ---------------------------------------------------------------- response mapping

    @Test
    void response_isMappedByColumnNameNotByPosition() throws Exception {
        api.reply = reply(
            List.of("properties", "extra", "distinct_id", "event", "timestamp", "uuid"),
            List.of(List.of("{\"k\":\"v\"}", "ignored", "user-9", "signup", "2026-01-01T10:00:03Z", "uuid-9"))
        );
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        Map<String, Object> event = events(poll.run().orElseThrow()).getFirst();

        assertEquals("uuid-9", event.get("id"));
        assertEquals("signup", event.get("event"));
        assertEquals("user-9", event.get("distinctId"));
        assertEquals(BASE.plusSeconds(3), timestamp(event));
        assertEquals(Map.of("k", "v"), event.get("properties"));
    }

    @Test
    void properties_areMappedFromAJsonStringAnObjectOrNull() throws Exception {
        List<String> columns = List.of("uuid", "event", "distinct_id", "timestamp", "properties");
        api.reply = reply(columns, List.of(
            List.of("e1", "signup", "u", "2026-01-01T10:00:01Z", "{\"a\":1,\"nested\":{\"b\":[1,2]}}"),
            Arrays.asList("e2", "signup", "u", "2026-01-01T10:00:02Z", Map.of("a", 2)),
            Arrays.asList("e3", "signup", "u", "2026-01-01T10:00:03Z", null)
        ));
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        List<Map<String, Object>> events = events(poll.run().orElseThrow());

        assertEquals(Map.of("a", 1, "nested", Map.of("b", List.of(1, 2))), events.get(0).get("properties"));
        assertEquals(Map.of("a", 2), events.get(1).get("properties"));
        assertEquals(Map.of(), events.get(2).get("properties"));
    }

    @Test
    void timestamps_areMappedFromIsoWithAndWithoutOffsetOrZone() throws Exception {
        api.reply = reply(
            List.of("uuid", "event", "distinct_id", "timestamp", "properties"),
            List.of(
                List.of("e1", "signup", "u", "2026-01-01T10:00:01.123456Z", "{}"),
                List.of("e2", "signup", "u", "2026-01-01 10:00:02.5", "{}"),
                List.of("e3", "signup", "u", "2026-01-01T12:00:03+02:00", "{}")
            )
        );
        Poll poll = poll(trigger().build());
        seed(poll, BASE, MIN_UUID);

        List<Map<String, Object>> events = events(poll.run().orElseThrow());

        assertEquals(Instant.parse("2026-01-01T10:00:01.123456Z"), timestamp(events.get(0)));
        assertEquals(Instant.parse("2026-01-01T10:00:02.500Z"), timestamp(events.get(1)));
        assertEquals(Instant.parse("2026-01-01T10:00:03Z"), timestamp(events.get(2)));
        // the cursor keeps the microseconds of the last event
        assertEquals(Instant.parse("2026-01-01T10:00:03Z"), Instant.parse((String) cursor(poll).get("timestamp")));
    }

    // ---------------------------------------------------------------- secrets and defaults

    @Test
    void personalApiKey_isASecretAndNeverInToString() throws Exception {
        NewEvent trigger = trigger().build();

        PluginProperty annotation = NewEvent.class.getDeclaredField("personalApiKey").getAnnotation(PluginProperty.class);

        assertNotNull(annotation);
        assertTrue(annotation.secret());
        assertFalse(trigger.toString().contains(API_KEY), "toString must not expose the personal API key");
    }

    // ---------------------------------------------------------------- helpers

    private NewEvent.NewEventBuilder<?, ?> trigger() {
        return NewEvent.builder()
            .id("new-event-" + UUID.randomUUID())
            .type(NewEvent.class.getName())
            .personalApiKey(Property.ofValue(API_KEY))
            .projectId(Property.ofValue(123L))
            .host(Property.ofValue(api.url()));
    }

    private Poll poll(NewEvent trigger) {
        Map.Entry<ConditionContext, io.kestra.core.models.triggers.Trigger> entry = TestsUtils.mockTrigger(runContextFactory, trigger);
        return new Poll(trigger, entry.getKey(), entry.getValue());
    }

    private record Poll(NewEvent trigger, ConditionContext conditionContext, TriggerContext triggerContext) {
        Optional<Execution> run() throws Exception {
            return trigger.evaluate(conditionContext, triggerContext);
        }

        KVStore kv() {
            return conditionContext.getRunContext().namespaceKv(triggerContext.getNamespace());
        }

        String key() {
            return StatefulTriggerService.defaultKey(triggerContext.getNamespace(), triggerContext.getFlowId(), triggerContext.getTriggerId());
        }
    }

    private static void seed(Poll poll, Instant timestamp, String uuid) throws Exception {
        byte[] bytes = JacksonMapper.ofJson().writeValueAsString(Map.of("timestamp", timestamp.toString(), "uuid", uuid)).getBytes(StandardCharsets.UTF_8);
        poll.kv().put(poll.key(), new KVValueAndMetadata(new KVMetadata("test cursor", (Duration) null), bytes));
    }

    private static Map<String, Object> cursor(Poll poll) throws Exception {
        Object value = poll.kv().getValue(poll.key()).orElseThrow().value();
        return JacksonMapper.ofJson().readValue(value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(value), MAP_TYPE);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> events(Execution execution) {
        return (List<Map<String, Object>>) execution.getTrigger().getVariables().get("events");
    }

    private static Instant timestamp(Map<String, Object> event) {
        Object value = event.get("timestamp");
        return value instanceof Number number ? Instant.ofEpochMilli(number.longValue()) : Instant.parse(String.valueOf(value));
    }

    /**
     * Builds the property from JSON, like Kestra does when it reads a flow, so that the expression is really rendered.
     */
    private static <T> Property<T> templated(String expression, TypeReference<Property<T>> type) throws Exception {
        return JacksonMapper.ofJson().readValue(JacksonMapper.ofJson().writeValueAsString(expression), type);
    }

    private static MockPosthogApi.Reply reply(List<String> columns, List<List<Object>> rows) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("columns", columns);
        body.put("results", rows);
        body.put("hasMore", false);
        try {
            return new MockPosthogApi.Reply(200, JacksonMapper.ofJson().writeValueAsString(body), Map.of());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return (Map<String, Object>) value;
    }

    /**
     * A local stand-in for the PostHog Query API. Without a scripted {@code reply}, it evaluates the keyset query
     * (cursor, optional event name, limit) on an in-memory events table, like PostHog is expected to.
     */
    private static class MockPosthogApi {
        private static final Pattern QUERY_PATH = Pattern.compile(".*/api/projects/\\d+/query/");
        private static final Pattern LIMIT = Pattern.compile("LIMIT (\\d+)");

        record StoredEvent(String uuid, String event, String distinctId, Instant timestamp, String properties) {
        }

        record Reply(int status, String body, Map<String, String> headers) {
        }

        record Received(String method, String path, Map<String, String> headers, Map<String, Object> body) {
            String query() {
                return (String) ((Map<?, ?>) body.get("query")).get("query");
            }
        }

        final List<StoredEvent> events = new CopyOnWriteArrayList<>();
        final List<Received> requests = new CopyOnWriteArrayList<>();
        volatile Reply reply;

        private final HttpServer httpServer;

        MockPosthogApi() throws Exception {
            httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
            httpServer.createContext("/", exchange -> {
                byte[] raw = exchange.getRequestBody().readAllBytes();
                Map<String, String> headers = new LinkedHashMap<>();
                exchange.getRequestHeaders().forEach((name, values) -> headers.put(name.toLowerCase(), values.getFirst()));
                String path = exchange.getRequestURI().getPath();

                Reply answer;
                if (!QUERY_PATH.matcher(path).matches() || !"POST".equals(exchange.getRequestMethod())) {
                    answer = new Reply(404, "{\"detail\":\"Not found.\"}", Map.of());
                } else {
                    Map<String, Object> body = JacksonMapper.ofJson().readValue(new String(raw, StandardCharsets.UTF_8), MAP_TYPE);
                    requests.add(new Received(exchange.getRequestMethod(), path, headers, body));
                    answer = reply != null ? reply : evaluate(body);
                }

                byte[] response = answer.body().getBytes(StandardCharsets.UTF_8);
                answer.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                if (response.length == 0) {
                    exchange.sendResponseHeaders(answer.status(), -1);
                } else {
                    exchange.sendResponseHeaders(answer.status(), response.length);
                    exchange.getResponseBody().write(response);
                }
                exchange.close();
            });
            httpServer.start();
        }

        void add(String uuid, String event, Instant timestamp) {
            add(uuid, event, "user", timestamp, "{}");
        }

        void add(String uuid, String event, String distinctId, Instant timestamp, String properties) {
            events.add(new StoredEvent(uuid, event, distinctId, timestamp, properties));
        }

        Received lastRequest() {
            assertFalse(requests.isEmpty(), "no request reached the server");
            return requests.getLast();
        }

        private Reply evaluate(Map<String, Object> body) {
            Map<String, Object> query = map(body.get("query"));
            Map<String, Object> values = map(query.get("values"));
            Instant cursorTs = LocalDateTime.parse(((String) values.get("cursorTs")).replace(' ', 'T')).toInstant(ZoneOffset.UTC);
            String cursorUuid = (String) values.get("cursorUuid");
            String eventName = (String) values.get("eventName");
            Matcher limit = LIMIT.matcher((String) query.get("query"));
            assertTrue(limit.find(), "the query has no LIMIT");

            List<List<Object>> rows = events.stream()
                .filter(e -> !e.timestamp().isBefore(cursorTs) && (e.timestamp().isAfter(cursorTs) || e.uuid().compareTo(cursorUuid) > 0))
                .filter(e -> eventName == null || e.event().equals(eventName))
                .sorted(Comparator.comparing(StoredEvent::timestamp).thenComparing(StoredEvent::uuid))
                .limit(Long.parseLong(limit.group(1)))
                .map(e -> List.<Object>of(e.uuid(), e.event(), e.distinctId(), e.timestamp().toString(), e.properties()))
                .toList();

            return reply(List.of("uuid", "event", "distinct_id", "timestamp", "properties"), rows);
        }

        int port() {
            return httpServer.getAddress().getPort();
        }

        String url() {
            return "http://127.0.0.1:" + port();
        }

        void stop() {
            httpServer.stop(0);
        }
    }
}
