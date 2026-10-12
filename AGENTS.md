# Kestra PostHog Plugin

## What

- Provides plugin components under `io.kestra.plugin.posthog`.
- Tasks: `Capture`, `CaptureBatch`, `Identify`, `Alias`, `Group`, `EvaluateFeatureFlag`.
- Trigger: `NewEvent`.

## Why

- What user problem does this solve? Teams that send events to PostHog from pipelines and backend automation often do it with ad-hoc scripts or custom HTTP calls, which are hard to reuse and to observe.
- Why would a team adopt this plugin in a workflow? It keeps PostHog steps in the same Kestra flow as the rest of the pipeline, with Kestra expressions, secrets and execution history, and without custom scripting.
- What operational/business outcome does it enable? Teams can correlate workflow runs with product analytics: instrument pipelines with PostHog events, update users and groups, evaluate feature flags, and react to new events from Kestra.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `posthog`

The tasks send events and evaluate feature flags through the official PostHog Java SDK (`com.posthog:posthog-server`). The `NewEvent` trigger reads events through the PostHog Query API (HogQL) with Kestra's HTTP client.

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.posthog.AbstractPosthogTask`: base class of the SDK-based tasks. It holds the project API key (`apiKey`) and the `host`, and manages the PostHog client lifecycle (create, run, flush, close).
- `io.kestra.plugin.posthog.Capture`: sends one event.
- `io.kestra.plugin.posthog.CaptureBatch`: sends many events from a list or a file.
- `io.kestra.plugin.posthog.Identify`: sets person properties.
- `io.kestra.plugin.posthog.Alias`: links two identifiers of the same user.
- `io.kestra.plugin.posthog.Group`: creates or updates a group and its properties.
- `io.kestra.plugin.posthog.EvaluateFeatureFlag`: evaluates a feature flag for a user.
- `io.kestra.plugin.posthog.NewEvent`: polling trigger that returns new events. It extends `AbstractTrigger`, not `AbstractPosthogTask`, and uses a personal API key and a project ID instead of the project API key.

### Project Structure

```
plugin-posthog/
├── src/main/java/io/kestra/plugin/posthog/
├── src/main/resources/ (metadata, doc and icons)
├── src/test/java/io/kestra/plugin/posthog/
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.
- Run `./gradlew test` and `./gradlew lintPluginDocs` before pushing; CI runs `./gradlew check`, which includes the documentation lint.
- Keep every task and trigger in the root package `io.kestra.plugin.posthog`: the documentation lint rejects plugins split between the root package and subpackages.
- A task without output declares `RunnableTask<VoidOutput>` and returns `null` from `run()`, never `new VoidOutput()`.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
