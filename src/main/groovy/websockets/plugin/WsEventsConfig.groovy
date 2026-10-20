
package websockets.plugin

import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description



@ScopeName('wsEvents')
@Description('Configuration for the WebSocket event publishing plugin')
class WsEventsConfig implements ConfigScope {

    ProcessEvents events

    WsEventsConfig() {}

    WsEventsConfig(Map opts) {
        this.events = new ProcessEvents(opts.events as Map ?: [:])
    }

    @ConfigOption
    @Description('Enable or disable WebSocket event publishing')
    Boolean enabled = false

    @ConfigOption
    @Description('WebSocket endpoint URL')
    String url = 'ws://localhost:7777'

    @ConfigOption
    @Description('REST endpoint to register with websocket and receive url')
    String url_register = 'http://localhost:7777'

    @ConfigOption
    @Description('REST endpoint to register with websocket and receive url')
    String url_publish = 'http://localhost:7777'


    @ConfigOption
    @Description('Optional bearer token')
    String token = ''

    @ConfigOption
    @Description('Event topic')
    String topic = 'nextflow'

    @ConfigOption
    @Description('Optional workflow run ID override')
    String runId = ''

    @ConfigOption
    @Description('Fail the workflow when sending an event fails')
    Boolean failOnSendError = false


}

 /* ---------------------------  event flags  --------------------------- */
@Description('Event‑specific flags')
class ProcessEvents implements ConfigScope {
    
    @ConfigOption
    @Description('Publish workflow lifecycle events')
    Boolean workflowEvents = true

    @ConfigOption
    @Description('Publish process‑start events')
    Boolean processStart = true

    @ConfigOption
    @Description('Publish process‑complete events')
    Boolean processComplete = true

    @ConfigOption
    @Description('Publish cached events')
    Boolean processCached = true
}

