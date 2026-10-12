package io.kestra.plugin.posthog;

import com.posthog.server.PostHogCaptureOptions;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Capture a batch of PostHog events",
    description = """
        Sends many analytics events in a single task, using the PostHog SDK's internal batching (`POST /batch`). \
        Each item of `from` is an event with a required `distinctId` and `event`, and optional `properties` and `timestamp`. \
        All items are validated before anything is sent, so an invalid item means that nothing is captured. \
        Uses the project API key (`phc_`)."""
)
@Plugin(
    examples = {
        @Example(
            title = "Capture several events at once",
            full = true,
            code = """
                id: posthog_capture_batch
                namespace: company.analytics

                tasks:
                  - id: capture_batch
                    type: io.kestra.plugin.posthog.CaptureBatch
                    apiKey: "{{ secret('POSTHOG_API_KEY') }}"
                    host: "https://us.i.posthog.com"
                    from:
                      - distinctId: user_1
                        event: pipeline_completed
                        properties:
                          pipeline_name: etl
                      - distinctId: user_2
                        event: pipeline_completed
                        timestamp: "2026-01-01T00:00:00Z"
                """
        )
    }
)
public class CaptureBatch extends AbstractPosthogTask implements RunnableTask<CaptureBatch.Output>, Data.From {
    private static final int DEFAULT_BATCH_SIZE = 1000;

    @Schema(
        title = Data.From.TITLE,
        description = Data.From.DESCRIPTION + """


            Each item is an event with the following keys: `distinctId` (required), `event` (required), \
            `properties` (optional map) and `timestamp` (optional ISO-8601 date-time with an offset, e.g. `2026-01-01T00:00:00Z`). \
            Any other key is ignored."""
    )
    @NotNull
    @PluginProperty(group = "main")
    @ToString.Exclude
    private Object from;

    @Schema(
        title = "Maximum number of events per request",
        description = "Maximum number of events the PostHog SDK sends in one `POST /batch` request. Must be at least 1. Inputs with at most this many events are sent in a single request.",
        defaultValue = "1000"
    )
    @Builder.Default
    private Property<Integer> batchSize = Property.ofValue(DEFAULT_BATCH_SIZE);

    @Override
    public Output run(RunContext runContext) throws Exception {
        int renderedBatchSize = runContext.render(this.batchSize).as(Integer.class).orElse(DEFAULT_BATCH_SIZE);
        if (renderedBatchSize < 1) {
            throw new IllegalArgumentException("The PostHog `batchSize` must be at least 1 but was " + renderedBatchSize);
        }

        List<Map<String, Object>> rows;
        try {
            rows = Data.from(this.from).read(runContext).collectList().block();
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Each item of `from` must be an object with `distinctId` and `event`", e);
        }

        // validate every event before creating the client: if one is invalid, nothing is sent
        List<BatchEvent> events = new ArrayList<>();
        if (rows != null) {
            for (int index = 0; index < rows.size(); index++) {
                events.add(parseEvent(rows.get(index), index));
            }
        }

        int count = events.size();
        if (count == 0) {
            return Output.builder().eventsCount(0).build();
        }

        // never flush in the background while queuing: the flush done by the parent then sends everything,
        // in chunks of `batchSize`, and only returns once all requests are done.
        // `flushIntervalSeconds` is deliberately long: the SDK's default 5s timer sends a batch regardless of `flushAt`,
        // and if that request is still in flight when the final flush()/close() run, they return early and events are lost.
        // 3600 and not Integer.MAX_VALUE, as the SDK multiplies this value by 1000 in an int.
        withClient(
            runContext,
            builder -> builder
                .flushAt(count + 1)
                .maxQueueSize(count + 1)
                .maxBatchSize(renderedBatchSize)
                .flushIntervalSeconds(3600),
            client -> {
                for (BatchEvent event : events) {
                    PostHogCaptureOptions.Builder options = PostHogCaptureOptions.builder().properties(event.properties());
                    if (event.timestamp() != null) {
                        options.timestamp(event.timestamp());
                    }
                    client.capture(event.distinctId(), event.event(), options.build());
                }
                return null;
            }
        );

        return Output.builder().eventsCount(count).build();
    }

    private static BatchEvent parseEvent(Map<String, Object> row, int index) {
        return new BatchEvent(
            requiredText(row, "distinctId", index),
            requiredText(row, "event", index),
            parseProperties(row.get("properties"), index),
            parseTimestamp(row.get("timestamp"), index)
        );
    }

    private static String requiredText(Map<String, Object> row, String key, int index) {
        Object value = row.get(key);
        if (value == null) {
            throw new IllegalArgumentException("Event at index " + index + ": `" + key + "` is required");
        }
        if (!(value instanceof CharSequence || value instanceof Number)) {
            throw new IllegalArgumentException("Event at index " + index + ": `" + key + "` must be a string or a number but was " + value.getClass().getSimpleName());
        }

        String text = value.toString().trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException("Event at index " + index + ": `" + key + "` must not be blank");
        }
        return text;
    }

    private static Map<String, Object> parseProperties(Object value, int index) {
        if (value == null) {
            return Map.of();
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("Event at index " + index + ": `properties` must be an object but was " + value.getClass().getSimpleName());
        }

        Map<String, Object> properties = new LinkedHashMap<>();
        map.forEach((key, propertyValue) -> properties.put(String.valueOf(key), propertyValue));
        return properties;
    }

    private static Instant parseTimestamp(Object value, int index) {
        if (value == null) {
            return null;
        }
        if (value instanceof Instant instant) {
            return instant;
        }
        // typed date-times, e.g. read back from an internal storage file
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.toInstant();
        }
        if (value instanceof String text) {
            try {
                return OffsetDateTime.parse(text.trim()).toInstant();
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Event at index " + index + ": `timestamp` must be an ISO-8601 date-time with an offset, e.g. `2026-01-01T00:00:00Z`, but was '" + text + "'", e);
            }
        }
        throw new IllegalArgumentException("Event at index " + index + ": `timestamp` must be an ISO-8601 string or a date-time with an offset or zone but was " + value.getClass().getSimpleName());
    }

    private record BatchEvent(String distinctId, String event, Map<String, Object> properties, Instant timestamp) {
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of events handed to the PostHog SDK",
            description = "The number of events handed to the PostHog SDK, which sent them and waited for the requests to finish. This is not a confirmation that PostHog accepted them: the SDK does not report delivery failures."
        )
        private final Integer eventsCount;
    }
}
