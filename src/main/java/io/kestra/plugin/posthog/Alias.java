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

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Create a PostHog alias",
    description = """
        Tells PostHog that two identifiers belong to the same user, by sending a `$create_alias` event. \
        The `alias` identity is merged into the `distinctId` identity: `distinctId` is the user that is kept, \
        and both identifiers then resolve to that user. The merge is not applied by PostHog if the `alias` user \
        has already been identified. Uses the project API key (`phc_`)."""
)
@Plugin(
    examples = {
        @Example(
            title = "Link an anonymous ID to an authenticated user",
            full = true,
            code = """
                id: link_anonymous_user
                namespace: company.growth

                inputs:
                  - id: anonymous_id
                    type: STRING
                  - id: user_id
                    type: STRING

                tasks:
                  - id: alias_user
                    type: io.kestra.plugin.posthog.Alias
                    apiKey: "{{ secret('POSTHOG_API_KEY') }}"
                    host: "https://us.i.posthog.com"
                    distinctId: "{{ inputs.user_id }}"
                    alias: "{{ inputs.anonymous_id }}"
                """
        )
    }
)
public class Alias extends AbstractPosthogTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Distinct ID",
        description = "The identity of the user that is kept, e.g. the authenticated user ID. The `alias` identity is merged into it."
    )
    @NotNull
    private Property<String> distinctId;

    @Schema(
        title = "Alias",
        description = "The other identity of the same user, e.g. an anonymous ID. It is merged into the user identified by `distinctId`. Must be different from `distinctId`."
    )
    @NotNull
    private Property<String> alias;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String renderedDistinctId = runContext.render(this.distinctId).as(String.class).orElse("").trim();
        if (renderedDistinctId.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `distinctId` must not be blank");
        }

        String renderedAlias = runContext.render(this.alias).as(String.class).orElse("").trim();
        if (renderedAlias.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `alias` must not be blank");
        }

        // PostHog silently ignores an alias of an identity with itself
        if (renderedDistinctId.equals(renderedAlias)) {
            throw new IllegalArgumentException("The PostHog `distinctId` and `alias` must be different but both were '" + renderedDistinctId + "'");
        }

        withClient(runContext, client -> {
            client.alias(renderedDistinctId, renderedAlias);
            return null;
        });

        return null;
    }
}
