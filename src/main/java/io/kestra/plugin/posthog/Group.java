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
    title = "Create or update a PostHog group",
    description = """
        Creates a group, or updates the properties of an existing one, by sending a `$groupidentify` event. \
        A group is an entity such as a company or an organization, identified by its type and its key. \
        This does not make a user a member of the group: to attach events to a group, the events themselves must carry it. \
        Group analytics is a paid PostHog feature, and a project can have at most 5 group types. \
        Uses the project API key (`phc_`)."""
)
@Plugin(
    examples = {
        @Example(
            title = "Create a company group and set its properties",
            full = true,
            code = """
                id: define_company
                namespace: company.growth

                inputs:
                  - id: user_id
                    type: STRING
                  - id: company_id
                    type: STRING
                  - id: company_name
                    type: STRING

                tasks:
                  - id: define_company
                    type: io.kestra.plugin.posthog.Group
                    apiKey: "{{ secret('POSTHOG_API_KEY') }}"
                    host: "https://us.i.posthog.com"
                    distinctId: "{{ inputs.user_id }}"
                    groupType: company
                    groupKey: "{{ inputs.company_id }}"
                    groupProperties:
                      name: "{{ inputs.company_name }}"
                      plan: enterprise
                """
        )
    }
)
public class Group extends AbstractPosthogTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Distinct ID",
        description = "The distinct ID of the user the group-identify event is sent for. PostHog does not use it to link the user to the group."
    )
    @NotNull
    private Property<String> distinctId;

    @Schema(
        title = "Group type",
        description = "The category of the group, e.g. `company`. Prefer a singular name."
    )
    @NotNull
    private Property<String> groupType;

    @Schema(
        title = "Group key",
        description = "The unique identifier of the specific group within its type, e.g. a company ID."
    )
    @NotNull
    private Property<String> groupKey;

    @Schema(
        title = "Group properties",
        description = "Optional properties to store on the group. Properties with a null value are omitted."
    )
    private Property<Map<String, Object>> groupProperties;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        String renderedDistinctId = runContext.render(this.distinctId).as(String.class).orElse("").trim();
        if (renderedDistinctId.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `distinctId` must not be blank");
        }

        String renderedGroupType = runContext.render(this.groupType).as(String.class).orElse("").trim();
        if (renderedGroupType.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `groupType` must not be blank");
        }

        String renderedGroupKey = runContext.render(this.groupKey).as(String.class).orElse("").trim();
        if (renderedGroupKey.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `groupKey` must not be blank");
        }

        Map<String, Object> renderedProperties = runContext.render(this.groupProperties).asMap(String.class, Object.class);
        if (renderedProperties.keySet().stream().anyMatch(name -> name == null || name.isBlank())) {
            throw new IllegalArgumentException("The PostHog `groupProperties` must not contain a blank property name");
        }

        // without properties, the SDK omits `$group_set`
        Map<String, Object> propertiesOrNull = renderedProperties.isEmpty() ? null : renderedProperties;

        withClient(runContext, client -> {
            client.group(renderedDistinctId, renderedGroupType, renderedGroupKey, propertiesOrNull);
            return null;
        });

        return null;
    }
}
