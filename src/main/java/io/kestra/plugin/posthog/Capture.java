package io.kestra.plugin.posthog;

import com.posthog.server.PostHogCaptureOptions;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Capture a PostHog event",
    description = "Sends a custom analytics event for a `distinctId`, with optional properties and timestamp. Uses the project API key (`phc_`)."
)
@Plugin(
    examples = {
        @Example(
            title = "Capture a custom event when a pipeline completes",
            full = true,
            code = """
                id: pipeline_completed_event
                namespace: company.analytics

                inputs:
                  - id: user_id
                    type: STRING
                  - id: pipeline_name
                    type: STRING

                tasks:
                  - id: capture_completion
                    type: io.kestra.plugin.posthog.Capture
                    apiKey: "{{ secret('POSTHOG_API_KEY') }}"
                    host: "https://us.i.posthog.com"
                    distinctId: "{{ inputs.user_id }}"
                    event: pipeline_completed
                    properties:
                      pipeline_name: "{{ inputs.pipeline_name }}"
                      kestra_execution_id: "{{ execution.id }}"
                      environment: production
                """
        )
    }
)
public class Capture extends AbstractPosthogTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Distinct ID",
        description = "The distinct ID of the user performing the event."
    )
    @NotNull
    private Property<String> distinctId;

    @Schema(
        title = "Event name",
        description = "The name of the event to capture, e.g. `pipeline_completed`."
    )
    @NotNull
    private Property<String> event;

    @Schema(
        title = "Event properties",
        description = "Optional properties to attach to the event."
    )
    private Property<Map<String, Object>> properties;

    @Schema(
        title = "Event timestamp",
        description = "Optional time at which the event happened, as an ISO-8601 instant (e.g. `2026-01-01T00:00:00Z`). Defaults to the time the event is captured."
    )
    private Property<Instant> timestamp;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String renderedDistinctId = runContext.render(this.distinctId).as(String.class).orElse("").trim();
        if (renderedDistinctId.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `distinctId` must not be blank");
        }

        String renderedEvent = runContext.render(this.event).as(String.class).orElse("").trim();
        if (renderedEvent.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `event` must not be blank");
        }

        Map<String, Object> renderedProperties = runContext.render(this.properties).asMap(String.class, Object.class);
        Optional<Instant> renderedTimestamp = runContext.render(this.timestamp).as(Instant.class);

        PostHogCaptureOptions.Builder options = PostHogCaptureOptions.builder().properties(renderedProperties);
        if (renderedTimestamp.isPresent()) {
            options.timestamp(renderedTimestamp.get());
        }
        PostHogCaptureOptions captureOptions = options.build();

        withClient(runContext, client -> {
            client.capture(renderedDistinctId, renderedEvent, captureOptions);
            return null;
        });

        return null;
    }
}
