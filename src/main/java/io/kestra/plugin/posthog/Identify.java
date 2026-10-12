package io.kestra.plugin.posthog;

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

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Identify a PostHog user",
    description = """
        Sets or updates the person properties of the user identified by `distinctId`, by sending an `$identify` event \
        with the properties in `$set`. Uses the project API key (`phc_`). \
        Unlike the frontend `identify`, this does not merge an anonymous user into an identified one: use `Alias` for that."""
)
@Plugin(
    examples = {
        @Example(
            title = "Identify a user and set their properties",
            full = true,
            code = """
                id: user_onboarding_tracking
                namespace: company.growth

                inputs:
                  - id: user_id
                    type: STRING
                  - id: plan
                    type: STRING

                tasks:
                  - id: identify_user
                    type: io.kestra.plugin.posthog.Identify
                    apiKey: "{{ secret('POSTHOG_API_KEY') }}"
                    host: "https://us.i.posthog.com"
                    distinctId: "{{ inputs.user_id }}"
                    properties:
                      plan: "{{ inputs.plan }}"
                      onboarding_completed: true
                """
        )
    }
)
public class Identify extends AbstractPosthogTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Distinct ID",
        description = "The distinct ID of the user to identify."
    )
    @NotNull
    private Property<String> distinctId;

    @Schema(
        title = "Person properties",
        description = "The properties to set on the person, sent as `$set`. At least one property is required. Properties with a null value are omitted: a null does not unset a property."
    )
    @NotNull
    private Property<Map<String, Object>> properties;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String renderedDistinctId = runContext.render(this.distinctId).as(String.class).orElse("").trim();
        if (renderedDistinctId.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `distinctId` must not be blank");
        }

        Map<String, Object> renderedProperties = runContext.render(this.properties).asMap(String.class, Object.class);
        if (renderedProperties.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `properties` must contain at least one property");
        }
        if (renderedProperties.keySet().stream().anyMatch(name -> name == null || name.isBlank())) {
            throw new IllegalArgumentException("The PostHog `properties` must not contain a blank property name");
        }

        withClient(runContext, client -> {
            client.identify(renderedDistinctId, renderedProperties);
            return null;
        });

        return null;
    }
}
