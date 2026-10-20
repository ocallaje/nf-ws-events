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
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

/**
 * Implements a factory object required to create
 * the {@link WsEventsObserver} instance.
 */
@CompileStatic
class WsEventsFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        Map cfg = (Map) session.config.navigate('wsEvents') ?: [:]
        if (cfg.enabled != true) {
            return []
        }

        String url = cfg.url?.toString()
        String url_register = cfg.url_register?.toString()
        String token          = cfg.token?.toString()
        String topic          = cfg.topic?.toString()
        String runId          = cfg.runId?.toString()

        if (!url && !url_register) {
            throw new IllegalArgumentException(
                'wsEvents.enabled=true requires wsEvents.url OR wsEvents.url_register'
            )
        }

        Boolean failOnSendError = cfg.failOnSendError as Boolean
        if (failOnSendError == null) failOnSendError = false


        Map ev = (cfg.events ?: [:]) as Map
        Boolean workflowEvents   = ev.workflowEvents   as Boolean
        if (workflowEvents == null) workflowEvents   = true
        Boolean processStart     = ev.processStart     as Boolean
        if (processStart == null) processStart   = true
        Boolean processComplete  = ev.processComplete  as Boolean
        if (processComplete == null) processComplete   = true
        Boolean processCached    = ev.processCached    as Boolean
        if (processCached == null) processCached   = true

        return [new WsEventsObserver(
            url,
            url_register,
            token,
            topic,
            runId,
            failOnSendError,
            workflowEvents,
            processStart,
            processComplete,
            processCached,
        )]
    }

}
