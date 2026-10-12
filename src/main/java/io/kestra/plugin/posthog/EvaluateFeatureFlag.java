package io.kestra.plugin.posthog;

import com.posthog.server.PostHogEvaluateFlagsOptions;
import com.posthog.server.PostHogFeatureFlagEvaluations;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
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

import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Evaluate a PostHog feature flag",
    description = """
        Asks PostHog for the value of a feature flag for a given user. The result is the flag value: a boolean for a boolean flag, \
        or the variant name for a multivariate flag. A flag that PostHog does not return gives a null value, which is not an error. \
        The task fails if the evaluation request itself fails (invalid API key, unreachable host, server error...). \
        Reading the flag also sends a `$feature_flag_called` event to PostHog, as the PostHog SDK does by default. \
        Uses the project API key (`phc_`)."""
)
@Plugin(
    examples = {
        @Example(
            title = "Check a feature flag and use its value in a later task",
            full = true,
            code = """
                id: feature_flag_gate
                namespace: company.analytics

                inputs:
                  - id: user_id
                    type: STRING

                tasks:
                  - id: check_flag
                    type: io.kestra.plugin.posthog.EvaluateFeatureFlag
                    apiKey: "{{ secret('POSTHOG_API_KEY') }}"
                    host: "https://us.i.posthog.com"
                    distinctId: "{{ inputs.user_id }}"
                    flagKey: new-checkout

                  - id: log_flag
                    type: io.kestra.plugin.core.log.Log
                    message: "new-checkout is {{ outputs.check_flag.enabled ? 'on' : 'off' }} for {{ inputs.user_id }} (value: {{ outputs.check_flag.value }})"
                """
        )
    }
)
public class EvaluateFeatureFlag extends AbstractPosthogTask implements RunnableTask<EvaluateFeatureFlag.Output> {
    @Schema(
        title = "Distinct ID",
        description = "The distinct ID of the user to evaluate the feature flag for."
    )
    @NotNull
    private Property<String> distinctId;

    @Schema(
        title = "Flag key",
        description = "The key of the feature flag to evaluate, e.g. `new-checkout`."
    )
    @NotNull
    private Property<String> flagKey;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedDistinctId = runContext.render(this.distinctId).as(String.class).orElse("").trim();
        if (renderedDistinctId.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `distinctId` must not be blank");
        }

        String renderedFlagKey = runContext.render(this.flagKey).as(String.class).orElse("").trim();
        if (renderedFlagKey.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `flagKey` must not be blank");
        }

        return withClient(runContext, client -> {
            PostHogFeatureFlagEvaluations flags = client.evaluateFlags(
                renderedDistinctId,
                PostHogEvaluateFlagsOptions.builder().flagKeys(List.of(renderedFlagKey)).build()
            );

            // the SDK never throws when the request fails: it returns an empty snapshot, without a request id.
            // This is checked before reading the flag, so that no `$feature_flag_called` event is sent for a failed evaluation.
            if (flags.getRequestId() == null && flags.getKeys().isEmpty()) {
                throw new IllegalStateException("PostHog feature flag evaluation failed: check the apiKey, host, and PostHog server status.");
            }

            // read the flag only once, as each read queues a `$feature_flag_called` event
            Object value = flags.getFlag(renderedFlagKey);

            if (value == null) {
                runContext.logger().warn("The feature flag '{}' was not returned by PostHog", renderedFlagKey);
                return Output.builder().value(null).enabled(false).variant(null).build();
            }
            if (value instanceof Boolean enabled) {
                return Output.builder().value(enabled).enabled(enabled).variant(null).build();
            }
            if (value instanceof String variant) {
                return Output.builder().value(variant).enabled(!variant.isEmpty()).variant(variant.isEmpty() ? null : variant).build();
            }
            throw new IllegalStateException("Unexpected value type for the feature flag '" + renderedFlagKey + "': " + value.getClass().getSimpleName());
        });
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Flag value",
            description = "The value of the flag: a boolean for a boolean flag, the variant name for a multivariate flag, or null if PostHog did not return the flag.",
            anyOf = {Boolean.class, String.class}
        )
        private final Object value;

        @Schema(
            title = "Whether the flag is enabled",
            description = "True for an enabled boolean flag or a multivariate flag with a variant. False for a disabled flag, and for a flag that PostHog did not return."
        )
        private final Boolean enabled;

        @Schema(
            title = "Variant",
            description = "The variant name of a multivariate flag, or null for a boolean flag or a flag that PostHog did not return."
        )
        private final String variant;
    }
}
