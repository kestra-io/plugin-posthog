package io.kestra.plugin.posthog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.kv.KVMetadata;
import io.kestra.core.storages.kv.KVStore;
import io.kestra.core.storages.kv.KVValue;
import io.kestra.core.storages.kv.KVValueAndMetadata;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow when new PostHog events appear",
    description = """
        Polls the PostHog Query API (HogQL) for events that appeared since the previous poll, and starts one execution with the new events. \
        Uses a **personal API key** and the numeric **project ID**, which are different from the project API key (`phc_`) of the other PostHog tasks: \
        the personal key needs the `Query Read` permission, and `host` is the PostHog app host (`https://us.posthog.com`, `https://eu.posthog.com` or your self-hosted URL), \
        not the ingestion host.

        Events are read in order of their `timestamp` (the time the event happened, not the time PostHog received it), \
        using the pair `timestamp` and `uuid` as a cursor stored in the namespace KV store, so that events sharing a timestamp are neither repeated nor skipped. \
        The first poll only records the current time: events that happened before it are not returned. \
        An event with a `timestamp` earlier than the cursor, such as a backdated one, is never returned. \
        If more than `maxEvents` events are new, the oldest ones are returned and the others are returned by the following polls.

        PostHog rate-limits its Query API per project (240 requests per minute and 2400 per hour, plus an hourly read budget), \
        and does not support the Query API as a bulk export mechanism: this is why the default `interval` is five minutes. \
        Keep it at `PT5M` or longer."""
)
@Plugin(
    examples = {
        @Example(
            title = "Start a flow for each batch of new `user_signed_up` events",
            full = true,
            code = """
                id: react_to_posthog_event
                namespace: company.analytics

                triggers:
                  - id: on_new_signup_event
                    type: io.kestra.plugin.posthog.NewEvent
                    personalApiKey: "{{ secret('POSTHOG_PERSONAL_API_KEY') }}"
                    projectId: "{{ secret('POSTHOG_PROJECT_ID') }}"
                    host: "https://us.posthog.com"
                    eventName: user_signed_up
                    maxEvents: 100
                    interval: PT5M

                tasks:
                  - id: handle_events
                    type: io.kestra.plugin.core.flow.EachParallel
                    value: "{{ trigger.events }}"
                    tasks:
                      - id: log_event
                        type: io.kestra.plugin.core.log.Log
                        message: "New {{ fromJson(taskrun.value).event }} event for {{ fromJson(taskrun.value).distinctId }} at {{ fromJson(taskrun.value).timestamp }}"
                """
        )
    }
)
public class NewEvent extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<NewEvent.Output> {
    private static final String DEFAULT_HOST = "https://us.posthog.com";
    private static final int DEFAULT_MAX_EVENTS = 100;
    // the maximum number of rows PostHog returns for one API query
    private static final int MAX_EVENTS_LIMIT = 50_000;
    // the smallest UUID: with it, the cursor only compares timestamps
    private static final String MIN_UUID = "00000000-0000-0000-0000-000000000000";
    private static final DateTimeFormatter QUERY_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC);
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    @Builder.Default
    @Schema(
        title = "Interval between two polls",
        description = "The interval between two polls of the PostHog Query API. Defaults to five minutes because of the PostHog rate limits: keep `PT5M` or longer."
    )
    private final Duration interval = Duration.ofMinutes(5);

    @Schema(
        title = "PostHog personal API key",
        description = "A personal API key (`phx_`) with the `Query Read` permission. This is not the project API key (`phc_`) used by the other PostHog tasks. Use a Kestra secret."
    )
    @NotNull
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> personalApiKey;

    @Schema(
        title = "PostHog project ID",
        description = "The numeric ID of the PostHog project, as found in the project settings or in the URL of the PostHog app. Must be greater than 0."
    )
    @NotNull
    private Property<Long> projectId;

    @Builder.Default
    @Schema(
        title = "PostHog app host",
        description = "The PostHog app host: `https://us.posthog.com` (US Cloud), `https://eu.posthog.com` (EU Cloud) or the URL of a self-hosted instance. This is not the ingestion host (`us.i.posthog.com`) used by the other PostHog tasks.",
        defaultValue = DEFAULT_HOST
    )
    private Property<String> host = Property.ofValue(DEFAULT_HOST);

    @Schema(
        title = "Event name",
        description = "Optional name of the events to watch, e.g. `user_signed_up`. By default, events with any name are returned."
    )
    private Property<String> eventName;

    @Builder.Default
    @Schema(
        title = "Maximum number of events per poll",
        description = "The maximum number of events returned by a poll, between 1 and 50000. When more events are new, the oldest are returned first and the others by the following polls.",
        defaultValue = "100"
    )
    private Property<Integer> maxEvents = Property.ofValue(DEFAULT_MAX_EVENTS);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        String renderedApiKey = runContext.render(this.personalApiKey).as(String.class).orElse("").trim();
        if (renderedApiKey.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `personalApiKey` must not be blank");
        }

        Long renderedProjectId = runContext.render(this.projectId).as(Long.class).orElse(null);
        if (renderedProjectId == null || renderedProjectId <= 0) {
            throw new IllegalArgumentException("The PostHog `projectId` must be a number greater than 0 but was " + renderedProjectId);
        }

        int renderedMaxEvents = runContext.render(this.maxEvents).as(Integer.class).orElse(DEFAULT_MAX_EVENTS);
        if (renderedMaxEvents < 1 || renderedMaxEvents > MAX_EVENTS_LIMIT) {
            throw new IllegalArgumentException("The PostHog `maxEvents` must be between 1 and " + MAX_EVENTS_LIMIT + " but was " + renderedMaxEvents);
        }

        String renderedHost = runContext.render(this.host).as(String.class).orElse("").trim();
        URI uri = queryUri(renderedHost.isEmpty() ? DEFAULT_HOST : renderedHost, renderedProjectId);

        String renderedEventName = runContext.render(this.eventName).as(String.class).orElse("").trim();

        KVStore kvStore = runContext.namespaceKv(context.getNamespace());
        String stateKey = StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), context.getTriggerId());

        Optional<Cursor> storedCursor = readCursor(kvStore, stateKey);
        if (storedCursor.isEmpty()) {
            // first poll: only remember where we are, the events that happened before are not replayed
            Cursor initial = new Cursor(Instant.now(), MIN_UUID);
            writeCursor(kvStore, stateKey, initial);
            logger.info("No cursor found, starting to watch PostHog events from {}", initial.timestamp());
            return Optional.empty();
        }
        Cursor cursor = storedCursor.get();

        Map<String, Object> values = new LinkedHashMap<>();
        values.put("cursorTs", QUERY_TIMESTAMP.format(cursor.timestamp()));
        values.put("cursorUuid", cursor.uuid());
        StringBuilder query = new StringBuilder("SELECT uuid, event, distinct_id, timestamp, properties FROM events ")
            .append("WHERE timestamp >= {cursorTs} AND (timestamp > {cursorTs} OR uuid > {cursorUuid})");
        if (!renderedEventName.isEmpty()) {
            // bound as a query value, never concatenated in the query text
            query.append(" AND event = {eventName}");
            values.put("eventName", renderedEventName);
        }
        // the limit is a validated integer, not user text
        query.append(" ORDER BY timestamp ASC, uuid ASC LIMIT ").append(renderedMaxEvents);

        Map<String, Object> hogQlQuery = new LinkedHashMap<>();
        hogQlQuery.put("kind", "HogQLQuery");
        hogQlQuery.put("query", query.toString());
        hogQlQuery.put("values", values);
        hogQlQuery.put("modifiers", Map.of("convertToProjectTimezone", false));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", hogQlQuery);
        // the default is a cached result, which could hide new events
        body.put("refresh", "force_blocking");
        body.put("name", "Kestra PostHog NewEvent trigger");

        HttpRequest request = HttpRequest.builder()
            .uri(uri)
            .method("POST")
            .addHeader("Authorization", "Bearer " + renderedApiKey)
            .body(HttpRequest.JsonRequestBody.of(body))
            .build();

        HttpResponse<String> response;
        // allowFailed: the status codes are handled below
        try (HttpClient client = HttpClient.builder()
            .runContext(runContext)
            .configuration(HttpConfiguration.builder().allowFailed(Property.ofValue(true)).build())
            .build()) {
            response = client.request(request, String.class);
        } catch (HttpClientException | IOException e) {
            logger.warn("The PostHog query could not be sent to {}, it will be retried on the next poll: {}", uri, e.getMessage());
            return Optional.empty();
        }

        int status = response.getStatus().getCode();
        if (status == 429) {
            String retryAfter = response.getHeaders().firstValue("Retry-After").map(value -> " (Retry-After: " + value + ")").orElse("");
            logger.warn("PostHog rate limit reached{}: the poll is skipped and will be retried on the next interval. {}", retryAfter, detail(response.getBody()));
            return Optional.empty();
        }
        if (status >= 500) {
            logger.warn("PostHog answered with HTTP {}: the poll is skipped and will be retried on the next interval. {}", status, detail(response.getBody()));
            return Optional.empty();
        }
        if (status == 401) {
            throw new IllegalStateException("PostHog authentication failed (HTTP 401): check the `personalApiKey`. " + detail(response.getBody()));
        }
        if (status == 403) {
            throw new IllegalStateException("PostHog refused the request (HTTP 403): the personal API key needs the `Query Read` permission and access to the project " + renderedProjectId + ". " + detail(response.getBody()));
        }
        if (status == 404) {
            throw new IllegalStateException("PostHog could not find the Query API (HTTP 404): check the `projectId` " + renderedProjectId + " and the `host` '" + uri.getScheme() + "://" + uri.getAuthority() + "'. " + detail(response.getBody()));
        }
        if (status == 400) {
            throw new IllegalStateException("PostHog rejected the query (HTTP 400): " + detail(response.getBody()));
        }
        if (status < 200 || status >= 300) {
            throw new IllegalStateException("PostHog answered with an unexpected HTTP " + status + ": " + detail(response.getBody()));
        }

        List<Output.Event> events = parseEvents(response.getBody());
        if (events.isEmpty()) {
            return Optional.empty();
        }

        Output output = Output.builder().events(events).count(events.size()).build();

        // at-least-once: the cursor only moves once the execution has been built. If something fails before, the same events are returned by the next poll.
        Execution execution = TriggerService.generateExecution(this, conditionContext, context, output);
        Output.Event last = events.getLast();
        writeCursor(kvStore, stateKey, new Cursor(last.getTimestamp(), last.getId()));

        return Optional.of(execution);
    }

    static URI queryUri(String host, long projectId) {
        String base = host.trim().replaceAll("/+$", "");
        return URI.create(base + "/api/projects/" + projectId + "/query/");
    }

    private static List<Output.Event> parseEvents(String body) {
        Map<String, Object> json;
        try {
            json = JacksonMapper.ofJson().readValue(body, MAP_TYPE);
        } catch (JsonProcessingException | RuntimeException e) {
            throw new IllegalStateException("PostHog returned a response that is not valid JSON", e);
        }
        if (json == null || !(json.get("results") instanceof List<?> rows)) {
            throw new IllegalStateException("PostHog returned an unexpected response: no `results` list");
        }
        if (rows.isEmpty()) {
            return List.of();
        }
        if (!(json.get("columns") instanceof List<?> columns)) {
            throw new IllegalStateException("PostHog returned an unexpected response: no `columns` list");
        }

        int uuid = columnIndex(columns, "uuid");
        int event = columnIndex(columns, "event");
        int distinctId = columnIndex(columns, "distinct_id");
        int timestamp = columnIndex(columns, "timestamp");
        int properties = columnIndex(columns, "properties");

        List<Output.Event> events = new ArrayList<>();
        for (int index = 0; index < rows.size(); index++) {
            if (!(rows.get(index) instanceof List<?> row) || row.size() < columns.size()) {
                throw new IllegalStateException("PostHog returned an unexpected response: row " + index + " does not match the columns");
            }
            if (row.get(uuid) == null) {
                throw new IllegalStateException("PostHog returned an unexpected response: row " + index + " has no uuid");
            }
            events.add(Output.Event.builder()
                .id(String.valueOf(row.get(uuid)))
                .event(row.get(event) == null ? null : String.valueOf(row.get(event)))
                .distinctId(row.get(distinctId) == null ? null : String.valueOf(row.get(distinctId)))
                .timestamp(parseTimestamp(row.get(timestamp), index))
                .properties(parseProperties(row.get(properties), index))
                .build());
        }
        return events;
    }

    private static int columnIndex(List<?> columns, String name) {
        int index = columns.indexOf(name);
        if (index < 0) {
            throw new IllegalStateException("PostHog returned an unexpected response: no `" + name + "` column");
        }
        return index;
    }

    private static Instant parseTimestamp(Object value, int row) {
        if (value instanceof String text) {
            String trimmed = text.trim();
            try {
                return OffsetDateTime.parse(trimmed).toInstant();
            } catch (DateTimeParseException ignored) {
                // no offset: the query asks for UTC
            }
            try {
                return LocalDateTime.parse(trimmed.replace(' ', 'T')).toInstant(ZoneOffset.UTC);
            } catch (DateTimeParseException e) {
                throw new IllegalStateException("PostHog returned an unexpected response: row " + row + " has an invalid timestamp '" + text + "'", e);
            }
        }
        throw new IllegalStateException("PostHog returned an unexpected response: row " + row + " has no valid timestamp");
    }

    private static Map<String, Object> parseProperties(Object value, int row) {
        if (value == null) {
            return Map.of();
        }
        if (value instanceof String text) {
            if (text.isBlank()) {
                return Map.of();
            }
            try {
                return JacksonMapper.ofJson().readValue(text, MAP_TYPE);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("PostHog returned an unexpected response: row " + row + " has properties that are not a JSON object", e);
            }
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> properties = new LinkedHashMap<>();
            map.forEach((key, propertyValue) -> properties.put(String.valueOf(key), propertyValue));
            return properties;
        }
        throw new IllegalStateException("PostHog returned an unexpected response: row " + row + " has properties of an unexpected type");
    }

    private static String detail(String body) {
        if (body == null || body.isBlank()) {
            return "No details were returned.";
        }
        try {
            Object detail = JacksonMapper.ofJson().readValue(body, MAP_TYPE).get("detail");
            if (detail != null) {
                return String.valueOf(detail);
            }
        } catch (JsonProcessingException | RuntimeException ignored) {
            // not a JSON error body, the raw body is used
        }
        return body.length() > 500 ? body.substring(0, 500) + "..." : body;
    }

    private static Optional<Cursor> readCursor(KVStore kvStore, String key) throws Exception {
        Optional<KVValue> stored = kvStore.getValue(key);
        if (stored.isEmpty()) {
            return Optional.empty();
        }

        Object value = stored.get().value();
        Map<String, Object> json = switch (value) {
            case byte[] bytes -> JacksonMapper.ofJson().readValue(bytes, MAP_TYPE);
            case String text -> JacksonMapper.ofJson().readValue(text, MAP_TYPE);
            case Map<?, ?> map -> JacksonMapper.ofJson().convertValue(map, MAP_TYPE);
            default -> throw new IllegalStateException("The PostHog trigger cursor '" + key + "' has an unexpected type: " + value);
        };
        return Optional.of(new Cursor(Instant.parse(String.valueOf(json.get("timestamp"))), String.valueOf(json.get("uuid"))));
    }

    private static void writeCursor(KVStore kvStore, String key, Cursor cursor) throws IOException {
        byte[] bytes = JacksonMapper.ofJson().writeValueAsString(Map.of("timestamp", cursor.timestamp().toString(), "uuid", cursor.uuid())).getBytes(StandardCharsets.UTF_8);
        // no expiration: an expired cursor would silently skip or replay events
        kvStore.put(key, new KVValueAndMetadata(new KVMetadata("PostHog NewEvent trigger cursor", (Duration) null), bytes));
    }

    private record Cursor(Instant timestamp, String uuid) {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "New events",
            description = "The new events, from the oldest to the newest. Use `{{ trigger.events }}`."
        )
        private final List<Event> events;

        @Schema(
            title = "Number of new events",
            description = "The number of events in `events`."
        )
        private final Integer count;

        @Builder
        @Getter
        public static class Event {
            @Schema(title = "Event ID", description = "The UUID of the event.")
            private final String id;

            @Schema(title = "Event name")
            private final String event;

            @Schema(title = "Distinct ID", description = "The distinct ID of the user that sent the event.")
            private final String distinctId;

            @Schema(title = "Timestamp", description = "When the event happened, as reported by the client that sent it.")
            private final Instant timestamp;

            @Schema(title = "Properties", description = "The properties of the event.")
            private final Map<String, Object> properties;
        }
    }
}
