/*
 * Copyright 2025, Seqera Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package websockets.plugin

import groovy.transform.CompileStatic
import groovy.json.JsonOutput
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import nextflow.Session
import nextflow.plugin.extension.Function
import nextflow.plugin.extension.PluginExtensionPoint

/**
 * Implements a custom function which can be imported by
 * Nextflow scripts.
 */
@CompileStatic
class WsEventsExtension extends PluginExtensionPoint {

    /* ----- explicit SLF4J logger  ----- */
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(WsEventsExtension)

    private String runName
    private String exitCode
    private String publishUrl   // endpoint that accepts the event JSON
    private String token = '***' // optional bearer token
    private boolean failOnSendError
    private String topic = ''

    @Override
    protected void init(Session session) {
        runName = session.runName
        publishUrl = session.config.navigate('wsEvents.url_publish') as String
        token = session.config.navigate('wsEvents.token') as String
        failOnSendError = session.config.navigate('wsEvents.failOnSendError') as Boolean
        topic = session.config.navigate('wsEvents.topic') as String
    }

    /**
     * Say hello to the given target.
     *
     * @param target
     */
    @Function
    void sayHello(String target) {
        println "Hello, ${target}!"
    }

    /**
     * Publish a websocket event.
     *
     * @param user_id   recipient’s user ID (optional)
     * @param topic     event topic (e.g. 'nextflow')
     * @param step      step name (e.g. 'analysis')
     * @param status    status e.g. 'started' / 'completed'
     */
    @Function
    void publishEvent(
                String step, 
                String status, 
                Long user_id = null ) {

        Map message = [
            step    : step,
            status  : status,
            run_name: runName
        ]
        Map payload = [
            topic   : topic,
            message : message
        ]
        if (user_id) payload.user_id = user_id

        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(publishUrl))
                    .header('Content-Type', 'application/json')
                    .POST(HttpRequest.BodyPublishers.ofString(JsonOutput.toJson(payload)))

            if (token) {                      // optional bearer token
                builder.header('Authorization', "Bearer ${token}")
            }

            HttpResponse<String> resp = HttpClient.newHttpClient()
                   .send(builder.build(), BodyHandlers.ofString())

            if (resp.statusCode() in 200..299) {
                log.info("Published event: {}", payload)
            } else {
                log.warn("Publish failed ({}): {}", resp.statusCode(), resp.body())
                if (failOnSendError) throw new RuntimeException('Publish HTTP error')
            }
        } catch (Exception e) {
            log.warn('Could not publish event {}: {}', payload, e.toString())
            if (failOnSendError) throw new RuntimeException(e)
        }
    }

}
