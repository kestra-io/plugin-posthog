package io.kestra.plugin.posthog;

import com.posthog.server.PostHog;
import com.posthog.server.PostHogConfig;
import com.posthog.server.PostHogInterface;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.util.function.Consumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPosthogTask extends Task {
    @Schema(
        title = "PostHog project API key",
        description = "The project API key (starts with `phc_`). Use a Kestra secret, e.g. `{{ secret('POSTHOG_API_KEY') }}`."
    )
    @NotNull
    @PluginProperty(secret = true, group = "connection")
    @ToString.Exclude
    private Property<String> apiKey;

    @Schema(
        title = "PostHog host",
        description = "The PostHog ingestion host, e.g. `https://us.i.posthog.com` (US Cloud), `https://eu.i.posthog.com` (EU Cloud) or a self-hosted URL. Defaults to US Cloud when empty."
    )
    private Property<String> host;

    /**
     * Creates a PostHog client, runs the operation with it, then flushes and closes the client.
     * The client is closed even if the operation throws.
     */
    protected <T> T withClient(RunContext runContext, PosthogOperation<T> operation) throws Exception {
        return withClient(runContext, builder -> {
        }, operation);
    }

    /**
     * Same as {@link #withClient(RunContext, PosthogOperation)}, but lets the caller adjust the SDK batching settings
     * (e.g. {@code flushAt}, {@code maxQueueSize}, {@code maxBatchSize}) through the config builder.
     * The consumer is applied after the API key and host are set, and is not meant to change them.
     */
    protected <T> T withClient(RunContext runContext, Consumer<PostHogConfig.Builder> configurer, PosthogOperation<T> operation) throws Exception {
        PostHogInterface client = createClient(runContext, configurer);
        try {
            T result = operation.apply(client);
            client.flush();
            return result;
        } finally {
            client.close();
        }
    }

    /**
     * Renders {@code apiKey} and {@code host} and builds the PostHog client.
     * A blank API key is rejected here because the SDK would silently disable itself instead of failing.
     */
    protected PostHogInterface createClient(RunContext runContext, Consumer<PostHogConfig.Builder> configurer) throws Exception {
        String renderedApiKey = runContext.render(this.apiKey).as(String.class).orElse("").trim();
        if (renderedApiKey.isEmpty()) {
            throw new IllegalArgumentException("The PostHog `apiKey` must not be blank");
        }

        // a blank host makes the SDK fall back to its default (US Cloud)
        String renderedHost = runContext.render(this.host).as(String.class).orElse("").trim();

        PostHogConfig.Builder builder = PostHogConfig.builder(renderedApiKey)
            .host(renderedHost);
        configurer.accept(builder);

        return PostHog.with(builder.build());
    }

    @FunctionalInterface
    protected interface PosthogOperation<T> {
        T apply(PostHogInterface client) throws Exception;
    }
}
