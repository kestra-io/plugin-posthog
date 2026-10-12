<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

<p align="center">
  <a href="https://twitter.com/kestra_io" style="margin: 0 10px;">
        <img src="https://kestra.io/twitter.svg" alt="twitter" width="35" height="25" /></a>
  <a href="https://www.linkedin.com/company/kestra/" style="margin: 0 10px;">
        <img src="https://kestra.io/linkedin.svg" alt="linkedin" width="35" height="25" /></a>
  <a href="https://www.youtube.com/@kestra-io" style="margin: 0 10px;">
        <img src="https://kestra.io/youtube.svg" alt="youtube" width="35" height="25" /></a>
</p>

<br />
<p align="center">
    <a href="https://go.kestra.io/video/product-overview" target="_blank">
        <img src="https://kestra.io/startvideo.png" alt="Get started in 3 minutes with Kestra" width="640px" />
    </a>
</p>
<p align="center" style="color:grey;"><i>Get started with Kestra in 3 minutes.</i></p>

# Kestra PostHog Plugin

## Why

- What user problem does this solve? Teams that send events to PostHog from pipelines and backend automation often do it with ad-hoc scripts or custom HTTP calls, which are hard to reuse and to observe.
- Why would a team adopt this plugin in a workflow? It keeps PostHog steps in the same Kestra flow as the rest of the pipeline, with Kestra expressions, secrets and execution history, and without custom scripting.
- What operational/business outcome does it enable? Teams can correlate workflow runs with product analytics: instrument pipelines with PostHog events, update users and groups, evaluate feature flags, and react to new events from Kestra.

## What

- Provides plugin components under `io.kestra.plugin.posthog`.
- Tasks:
  - `Capture` sends one event.
  - `CaptureBatch` sends many events from a list or a file.
  - `Identify` sets person properties.
  - `Alias` links two identifiers of the same user.
  - `Group` creates or updates a group and its properties.
  - `EvaluateFeatureFlag` evaluates a feature flag for a user.
- Trigger:
  - `NewEvent` polls PostHog for new events and starts a flow with them.
- The tasks use the official PostHog Java SDK (`com.posthog:posthog-server`). `NewEvent` uses the PostHog Query API (HogQL).

## Authentication

PostHog uses two different credentials, and this plugin uses each of them in a different place:

- **Tasks** (`Capture`, `CaptureBatch`, `Identify`, `Alias`, `Group`, `EvaluateFeatureFlag`): the project API key (`phc_...`) as `apiKey`, and the PostHog ingestion `host`, for example `https://us.i.posthog.com`.
- **`NewEvent` trigger**: a personal API key (`phx_...`) as `personalApiKey`, the numeric `projectId`, and the PostHog application `host`, for example `https://us.posthog.com`. The project API key is not used here.

Provide keys through Kestra secrets, for example `{{ secret('POSTHOG_API_KEY') }}`. See [the plugin documentation](src/main/resources/doc/io.kestra.plugin.posthog.md) for the details of each task and of the trigger.

## Documentation
* Full documentation can be found under: [kestra.io/docs](https://kestra.io/docs)
* Documentation for developing a plugin is included in the [Plugin Developer Guide](https://kestra.io/docs/plugin-developer-guide/)


## License
Apache 2.0 © [Kestra Technologies](https://kestra.io)


## Stay up to date

We release new versions every month. Give the [main repository](https://github.com/kestra-io/kestra) a star to stay up to date with the latest releases and get notified about future updates.

![Star the repo](https://kestra.io/star.gif)
