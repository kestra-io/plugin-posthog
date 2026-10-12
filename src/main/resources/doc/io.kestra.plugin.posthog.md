# How to use the PostHog plugin

This plugin provides Kestra tasks to send events, identify users, manage groups and evaluate feature flags in PostHog, and a polling trigger that starts a flow when new events appear. Each task and the trigger have their own documentation page with full examples.

## Authentication

PostHog uses two different kinds of credentials, and this plugin uses them in two different places. They are not interchangeable.

### Tasks

`Capture`, `CaptureBatch`, `Identify`, `Alias`, `Group` and `EvaluateFeatureFlag` use the **project API key** (`phc_...`) and the PostHog **ingestion host**:

- `apiKey`: the project API key.
- `host`: `https://us.i.posthog.com` (US Cloud), `https://eu.i.posthog.com` (EU Cloud) or the URL of your self-hosted instance. When it is empty, the US Cloud host is used.

Store the key as a Kestra secret rather than writing it in the flow: `apiKey: "{{ secret('POSTHOG_API_KEY') }}"`.

### NewEvent trigger

`NewEvent` reads events from PostHog, so it does **not** use the project API key of the tasks. It uses a **personal API key**, the **project ID** and the PostHog **application host**:

- `personalApiKey`: a PostHog personal API key (`phx_...`) that is allowed to run queries (PostHog's `Query Read` permission).
- `projectId`: the numeric ID of the PostHog project.
- `host`: `https://us.posthog.com` (US Cloud, the default), `https://eu.posthog.com` (EU Cloud) or your self-hosted URL. This is the application host, not the ingestion host (`us.i.posthog.com`) used by the tasks.

Use secrets for both values: `personalApiKey: "{{ secret('POSTHOG_PERSONAL_API_KEY') }}"` and `projectId: "{{ secret('POSTHOG_PROJECT_ID') }}"`.

## Tasks

### Capture

Sends one event. `distinctId` and `event` are required; `properties` (a map) and `timestamp` (an ISO-8601 instant such as `2026-01-01T00:00:00Z`) are optional.

The task waits for the PostHog SDK to flush and close before it completes. The SDK does not report delivery failures, so a successful task means the event was handed to PostHog's client and sent, not that PostHog confirmed it.

### CaptureBatch

Sends many events from a single task, for bulk ingestion. The `from` property accepts a list of events, a single event, a JSON string, or the `kestra://` URI of a file in internal storage. Each event needs a `distinctId` and an `event`, and can have `properties` and a `timestamp` (ISO-8601 with an offset). All events are validated before anything is sent, so one invalid event means that nothing is captured.

Events are sent through PostHog's `/batch` endpoint, in requests of up to `batchSize` events (default `1000`, at least `1`). All events are kept in memory until they are sent. The task waits for the flush before it completes.

The `eventsCount` output is the number of events handed to the PostHog SDK. It is not a confirmation of server-side delivery.

### Identify

Sets properties on the person identified by `distinctId`. `properties` is required and must contain at least one property; property names cannot be blank. A property with a null value is left out and does not remove the property from the person.

### Alias

Tells PostHog that two identifiers belong to the same user. `distinctId` and `alias` are both required and must be different. `distinctId` is the user that is kept, and `alias` is the other identifier of that user.

PostHog applies the merge on its side and ignores it in some cases, for example when the alias user has already been identified. The task cannot see that outcome.

### Group

Creates a group, or updates the properties of an existing one. `groupType` (such as `company`) and `groupKey` (the identifier of the specific group) identify the group, `groupProperties` is optional, and `distinctId` is the user the group-identify event is sent for.

This task does **not** make the user a member of the group. It identifies the group itself; membership is expressed separately, through the `$groups` data carried by events. Group analytics is a paid PostHog feature, and a project is limited to 5 group types.

### EvaluateFeatureFlag

Evaluates one feature flag for one user. `distinctId` and `flagKey` are required. The output contains:

- `value`: `true` or `false` for a boolean flag, the variant name for a multivariate flag, or null when PostHog does not return the flag.
- `enabled`: whether the flag is on for this user.
- `variant`: the variant name, or null for a boolean flag.

A flag that PostHog does not return is not an error. If the evaluation request itself fails (invalid key, unreachable host, server error), the task fails instead of reporting the flag as disabled. As with the PostHog SDK by default, reading the flag also sends a `$feature_flag_called` event to PostHog.

## Triggers

### NewEvent

Polls PostHog for new events and starts one execution with the events found since the previous poll. It uses the PostHog Query API (HogQL), not the `/events/` endpoint, with the personal API key and project ID described above.

- `interval`: defaults to `PT5M`. PostHog rate-limits its Query API, so keep this interval or a longer one.
- `maxEvents`: the maximum number of events per poll, `100` by default, between `1` and `50000`. When more events are new, the oldest are returned first and the others by the following polls.
- `eventName`: optional, to only watch events with this name.

The execution receives `{{ trigger.events }}`, a list of events (`id`, `event`, `distinctId`, `timestamp`, `properties`), and `{{ trigger.count }}`. No execution is started when there is no new event.

The trigger keeps a cursor (the `timestamp` and `uuid` of the last event returned) in the namespace KV store, so that events are processed incrementally and events sharing a timestamp are not repeated. The first poll only records the starting point: events that happened before it are not processed. The cursor only moves once the execution has been created, so if creating it fails, the same events are returned again by the next poll. This is at-least-once behavior around that step only.

Limitations:

- Events are followed by their `timestamp`, the time reported for the event. An event with a timestamp earlier than the cursor, such as a backdated event, an event from a device with a skewed clock, or one that becomes queryable late, is not returned.
- The Query API is rate-limited per project (PostHog documents 240 requests per minute and 2400 per hour, plus an hourly read budget) and is not meant as a bulk export mechanism. Choose an `interval` and a `maxEvents` that fit your event volume.
- A rate limit (HTTP 429), a server error (5xx) or a network failure skips that poll and is retried at the next interval. Authentication, permission, unknown project and invalid query errors fail the trigger.
- Behavior against a live PostHog deployment, such as timestamp precision or query timeouts, is not covered by the automated tests, which use a local mock server. Treat these semantics as best effort.
