# nf‑ws‑events – Nextflow Task Status over WebSocket

**Summary**  
`nf‑ws‑events` is a Nextflow plugin that streams workflow‑ and task‑level lifecycle
events to a WebSocket endpoint in real time. It uses the built‑in Java
WebSocket client, so you don’t need any external tools or custom container
images. The plugin runs inside the Nextflow controller JVM; the WebSocket URL
must be reachable from that environment.

The plugin also allows for HTTP REST endpoint POST to send messages to a server using a custom function

---

## Get Started

### 1. Build / install locally

The project follows the standard Nextflow plugin template.

```bash
# Create a clean Nextflow‑plugin scaffold:
nextflow plugin create nf-ws-events websockets

# Copy this repository’s source into the scaffold:
cp -r src/main/groovy/* path/to/generated/project/src/main/groovy/
cp -r src/test/groovy/*  path/to/generated/project/src/test/groovy/

```

Build and install with Gradle:

```bash
cd nf-ws-events
make install
```

Or use it directly from the Nextflow Plugin Registry by adding it to your pipelines nextflow.config file:
See 2. Configure Nextflow

### 2. Configure Nextflow

Add the plugin and configuration to `nextflow.config`:

```groovy
plugins {
    id 'nf-ws-events' version '0.1.0'
}

wsEvents {
    enabled        = true                                    // set to false or omit to disable the plugin
    url            = 'wss://your-server.example/events'     // path to your websocket server
    url_register = 'http://localhost:7777/register'         // HTTP endpoint to register nextflow as a websocket client
    url_publish = 'http://localhost:7777/publish'           // HTTP endpoint for manual triggering of events
    topic = 'nextflow'                                      // websocket topic. can be empty

    // Optional settings
    token          = System.getenv('WS_EVENTS_TOKEN')
    runId          = '123'                                  // overrides session UUID. if used, must be numeric only
    failOnSendError = false                                 // instruct workflow to fail if websocket messages fail

    events {                                                // choose which events should emit websocket messages
        workflowEvents  = true                              // includes start and completion of workflows
        processStart    = true                              // triggers at the start of every process task
        processComplete = true                              // triggers at the completion of every process task
        processCached   = true                              // triggers on cached process tasks
    }
}
```

> **Tip** – Never commit secrets (such as the `token`) into the repo; reference
> environment variables instead.
> url or url_register must be used, both are not required
> optional settings can be omitted or set to null

---

## Events

Example task event:

The following is sent to the relevant 'topic'

```json
{
    "exit_code": 0,
    "run_id": "2567",
    "run_name": "gloomy_caravaggio",
    "status": "completed",
    "step": "SUMMARISE (4)",
    "task_id": "11",
    "task_tag": "4",
    "timestamp": "2026-10-20T17:41:28.413563445+01:00"
}
```

Example function call:

The following function is used to send a HTTP event to the url_publish endpoint

```groovy
publishEvent(step, stage, user_id)
```
Where step and stage are user defined strings. user_id is an integer or null.

The current implementation provides the following json body:
```json
{
    "user_id": "user_id", // sends to websocket client matching id. can be null to send to all
    "topic": "topic", // from nextflow.config
    "message": {
        "run_name": "gloomy_caravaggio",    // extracted from workflow
        "status": "stage",                  // provided to function
        "step": "step"                      // provided to function
    }
}

**Task statuses**

| Status     | Meaning |
|------------|---------|
| `started` | Task execution started |
| `completed` | Exit code 0 |
| `error` | Non‑zero exit code |
| `unknown` | No exit code exposed |
| `cached` (optional) | Cached task, emitted when `includeCached` is enabled |

If `workflowEvents` is set, the plugin will also emit `started`, `completed`,
and `error` events for the overall workflow.

---

## Server contract & limitations

* The current implementation opens a **new** WebSocket connection at the creation of the workflow. 
  This connection remains open and is reused for every event. The connection is closed at the 
  completion or termination of the workflow. The server must
  accept this pattern.
* It blocks the controller for each event until the send completes
  (timeout in the `WebSocketEndpoint` constructor).  
  This prevents race conditions when workflow completion events are triggered at the same time as the final process completion.
* Delivery is **best‑effort** – failures are logged and ignored unless
  `failOnSendError` is `true`, in which case the callback failure may
  interfere with workflow execution.
* The plugin does not guarantee events for abrupt controller termination or
  every executor submission failure.
* If your endpoint requires a persistent connection, sub‑protocol, or
  different authentication scheme, modify `WsEventsObserver.sendEvent()`.

---

## Tests
1. Tests can be run using make test
2. A test pipleine is implemented in validation. Test pipeline using
```bash
nextflow run validation/discover.nf
```
3. End-to-end testing is performed using
```bash
python3 validation/e2e_test.py
```
This initiates a python websocket server and events are tracked and counted based on provided nextflow.config


---

## License

Apache 2.0 – see the [LICENSE](LICENSE) file for details.

---

## What’s new

* Initial release – event emission for tasks and workflows,
  configurable via `nextflow.config`.


## Breaking changes

None yet. Future releases may modify the event schema or default timeouts;
always review the changelog before upgrading.

---```