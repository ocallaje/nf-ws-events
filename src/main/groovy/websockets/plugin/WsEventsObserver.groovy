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

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovy.json.JsonSlurper

import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

import java.math.BigInteger
import java.net.URI
import java.net.http.HttpClient
import java.net.http.WebSocket
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import java.net.InetAddress
import java.time.Duration
import java.time.OffsetDateTime
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit
import java.util.UUID
import java.util.concurrent.CompletableFuture

/**
 * Reports Nextflow task lifecycle events to a WebSocket endpoint.
 * One WebSocket connection is opened for each event; the server must support
 * one JSON text message per connection.
 */
@Slf4j
@CompileStatic
class WsEventsObserver implements TraceObserver {

    private final String url
    private final String url_register
    private final String token
    private final String topic
    private final String configuredRunId
    private final boolean failOnSendError
    private final boolean workflowEvents
    private final boolean processStart
    private final boolean processComplete
    private final boolean processCached



    private final HttpClient client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(8))
        .build()

    private String runId
    private String runName
    private String wsUrl
    private WebSocket mainSocket
    private final Object sendLock = new Object()

    WsEventsObserver(String url, String url_register, String token, String topic, String runId,
                            boolean failOnSendError, boolean workflowEvents,
                            boolean processStart, boolean processComplete,
                            boolean processCached) {
        this.url = url
        this.url_register = url_register
        this.token = token
        this.topic = topic
        this.configuredRunId = runId   
        this.failOnSendError = failOnSendError
        this.workflowEvents = workflowEvents
        this.processStart = processStart
        this.processComplete = processComplete
        this.processCached = processCached
    }

    @Override
    void onFlowCreate(Session session) {
        if (this.configuredRunId) {
            runId = configuredRunId
        } else {
            runId = (session.uniqueId?.toString() ?: UUID.randomUUID().toString()).replaceAll('-', '')
        }
        this.runName = session.runName
        /* ----------------- REGISTER WORKFLOW ----------------- */
        try {
            registerWorkflow(runId)
        } catch (Throwable e) {
            log.warn('Could not register workflow: {}', e.toString())
            if (failOnSendError) throw new RuntimeException(e)
        }


        if (workflowEvents) {
            sendEvent('workflow', 'started', null, null, null)
        }
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        if (processStart) {
            sendEvent(processName(handler, trace), 'started', taskId(trace), taskTag(trace), null)
        }
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        if (processComplete) {
            Integer code = exitCode(trace)
            // A missing exit code should not be incorrectly reported as success.
            String status = code == null ? 'unknown' : (code == 0 ? 'completed' : 'error')
            sendEvent(processName(handler, trace), status, taskId(trace), taskTag(trace), code)
        }
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        if (processCached) {
            sendEvent(processName(handler, trace), 'completed', taskId(trace),taskTag(trace), 0, true)
        }
    }

    @Override
    void onFlowComplete() {
        if (workflowEvents) {
            sendEvent('workflow', 'completed', null, null, null)
            closeSocket()
        }
    }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        if (workflowEvents) {
            sendEvent('workflow', 'error', taskId(trace), taskTag(trace), exitCode(trace))
            closeSocket()
        }
    }

    private static String processName(TaskHandler handler, TraceRecord trace) {
        try {
            return handler?.task?.name?.toString() ?: 'unknown'
        } catch (Throwable ignored) {
            return 'unknown'
        }
    }

    private static String taskId(TraceRecord trace) {
        try {
            return trace?.taskId?.toString()
        } catch (Throwable ignored) {
            return null
        }
    }

    private static String taskTag(TraceRecord trace) {
        try {
            return trace?.get('tag')?.toString()
        } catch (Throwable ignored) {
            return null
        }
    }

    private static Integer exitCode(TraceRecord trace) {
        try {
            def value = trace?.get('exit')
            if (value == null) return null
            if (value instanceof Number) return ((Number)value).intValue()
            String text = value.toString()
            return text.isInteger() ? text.toInteger() : null
        } catch (Throwable ignored) {
            return null
        }
    }

    private void sendEvent(String step, String status, String taskId, String taskTag,
                           Integer exitCode, boolean cached = false) {
        Map message = [
            run_id: runId,
            run_name: runName,
            step: step,
            status: status,
            timestamp: OffsetDateTime.now().toString()
        ]
        if (taskId) message.task_id = taskId
        if (taskTag) message.task_tag = taskTag
        if (exitCode != null) message.exit_code = exitCode
        if (cached) message.cached = true

        Map payload = [
            //topics: [topic],    // for updating client topics
            topic: topic,
            message: message
        ]

        if (mainSocket == null) {
        log.error('WebSocket not ready – event dropped: {}', payload)
        return
    }
        try {
            synchronized (sendLock) {
                mainSocket.sendText(
                    JsonOutput.toJson(payload),
                    true
                ).get(10, TimeUnit.SECONDS)
            }
            log.debug('Sent WebSocket event {}', payload)
        } catch (Throwable e) {
            log.warn('Could not send WebSocket event {}: {}', payload, e.toString())

            if (failOnSendError) {
                throw new RuntimeException('WebSocket event delivery failed', e)
            }
        }
    }

    private void closeSocket() {
    if (mainSocket) mainSocket.sendClose(WebSocket.NORMAL_CLOSURE,
                                         'workflow finished').get(5, TimeUnit.SECONDS)
}

    /**
     * Sends a registration request to {@code url_register} using a normal
     * HTTP POST, expects a JSON body containing the final WebSocket URL.
     * The URL is saved in {@code wsUrl} for subsequent events.
     */
    private void registerWorkflow(String runId) {

        BigInteger full = new BigInteger(runId, 16)
        long userIdU64 = full.and(BigInteger.ONE.shiftLeft(63).subtract(BigInteger.ONE)).longValue()

        Map payload = [
            user_id: userIdU64,
            topic  : topic
        ]

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url_register))
                .header('Content-Type', 'application/json')
                .POST(HttpRequest.BodyPublishers.ofString(JsonOutput.toJson(payload)))
                .build()

        HttpResponse<String> resp = client.send(request, BodyHandlers.ofString())

        if (resp.statusCode() in 200..299) {
            Map<String,Object> json = new JsonSlurper().parseText(resp.body()) as Map
            wsUrl = normaliseRegisteredUri(json?.url as String)

            // open socket and keep it open
            mainSocket = client.newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(8))
                .buildAsync(URI.create(wsUrl), new WebSocket.Listener() {
                    @Override void onOpen(WebSocket ws) { ws.request(1) }
                    @Override CompletionStage<?> onText(WebSocket ws,
                                                       CharSequence d,
                                                       boolean last) { ws.request(1); return null }
                    @Override void onError(WebSocket ws, Throwable e) 
                        {
                            log.warn('Could not open connection to websocket: {}', e.toString())
                        }
                }).get(10, TimeUnit.SECONDS)

            log.info('Registered workflow, new websocket URL: {}', wsUrl)
        } else {
            log.warn('Registration endpoint responded {}: {}', resp.statusCode(), resp.body())
        }
    }

    
    /**
    * Normalises the WebSocket URI that the registration endpoint returns.
    * Replaces a host of "0.0.0.0" (or a missing host) with the machine’s
    * local routable address so the Java WebSocket client can connect.
    */
    private static String normaliseRegisteredUri(String rawUri) {
        URI uri = URI.create(rawUri)
        String host = uri.host
        if (host == '0.0.0.0' || host == null) {
            host = InetAddress.getLocalHost().hostAddress
        }
        return new URI(uri.scheme, uri.userInfo, host,
                    uri.port, uri.path, uri.query, uri.fragment).toString()
    }

}
