@PluginSubGroup(
    title = "PostHog",
    description = "Tasks that capture single or batched PostHog analytics events, identify users, create aliases and groups, and evaluate feature flags, plus a trigger that polls for new events through the PostHog Query API.",
    categories = PluginSubGroup.PluginCategory.BUSINESS
)
package io.kestra.plugin.posthog;

import io.kestra.core.models.annotations.PluginSubGroup;