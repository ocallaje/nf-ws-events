package websockets.plugin

import groovy.json.JsonSlurper
import nextflow.processor.TaskHandler
import nextflow.trace.TraceRecord
import spock.lang.Specification

import java.net.http.WebSocket
import java.util.concurrent.CompletableFuture

class WsEventsObserverSpec extends Specification {

    private static final String TOPIC = 'test-topic'
    private static final String RUN_ID = 'test-run-123'
    private static final String RUN_NAME = 'test-workflow'

    private WsEventsObserver createObserver(
            boolean workflowEvents = true,
            boolean processStart = true,
            boolean processComplete = true,
            boolean processCached = true,
            boolean failOnSendError = false
    ) {
        def observer = new WsEventsObserver(
                null,
                null,
                null,
                TOPIC,
                RUN_ID,
                failOnSendError,
                workflowEvents,
                processStart,
                processComplete,
                processCached
        )

        // Set workflow context without invoking onFlowCreate(),
        // which performs a real HTTP registration request.
        setField(observer, 'runId', RUN_ID)
        setField(observer, 'runName', RUN_NAME)

        return observer
    }

    private static void setField(Object target, String name, Object value) {
        def field = target.class.getDeclaredField(name)
        field.accessible = true
        field.set(target, value)
    }

    private static Map parsePayload(CharSequence text) {
        return new JsonSlurper().parseText(text.toString()) as Map
    }

    private static boolean matchesPayload(
            CharSequence text,
            Map expectedMessage
    ) {
        Map payload = parsePayload(text)

        assert payload.topic == TOPIC
        assert payload.message instanceof Map

        Map message = payload.message as Map

        assert message.run_id == RUN_ID
        assert message.run_name == RUN_NAME

        expectedMessage.each { key, value ->
            assert message[key] == value
        }

        assert message.timestamp != null
        return true
    }

    def 'sends process started event'() {
        given:
        def observer = createObserver()
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        when:
        observer.onProcessStart(null, null)

        then:
        1 * socket.sendText({
            matchesPayload(it as CharSequence, [
                step  : 'unknown',
                status: 'started'
            ])
        }, true) >> CompletableFuture.completedFuture(socket)
    }

    def 'sends process completed event when exit code is zero'() {
        given:
        def observer = createObserver()
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        def trace = Stub(TraceRecord) {
            getTaskId() >> 42L
            get('tag') >> 'sample-tag'
            get('exit') >> 0
        }

        when:
        observer.onProcessComplete(null, trace)

        then:
        1 * socket.sendText({
            Map payload = new JsonSlurper().parseText(it.toString()) as Map
            Map message = payload.message as Map

            assert payload.topic == TOPIC
            assert message.run_id == RUN_ID
            assert message.run_name == RUN_NAME
            assert message.step == 'unknown'
            assert message.status == 'completed'
            assert message.task_tag == 'sample-tag'
            assert message.exit_code == 0
            assert message.timestamp != null

            true
        }, true) >> CompletableFuture.completedFuture(socket)
    }

    def 'sends process error event for nonzero exit code'() {
        given:
        def observer = createObserver()
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        def trace = Stub(TraceRecord) {
            get('exit') >> 1
        }

        when:
        observer.onProcessComplete(null, trace)

        then:
        1 * socket.sendText({
            matchesPayload(it as CharSequence, [
                step    : 'unknown',
                status  : 'error',
                exit_code: 1
            ])
        }, true) >> CompletableFuture.completedFuture(socket)
    }

    def 'uses unknown status when exit code is missing'() {
        given:
        def observer = createObserver()
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        def trace = Stub(TraceRecord) {
            get('exit') >> null
        }

        when:
        observer.onProcessComplete(null, trace)

        then:
        1 * socket.sendText({
            matchesPayload(it as CharSequence, [
                step   : 'unknown',
                status : 'unknown',
            ])
        }, true) >> CompletableFuture.completedFuture(socket)
    }

    def 'marks cached process events as cached'() {
        given:
        def observer = createObserver()
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        def trace = Stub(TraceRecord) {
            get('tag') >> 'cached-task'
        }

        when:
        observer.onProcessCached(null, trace)

        then:
        1 * socket.sendText({
            Map payload = parsePayload(it as CharSequence)
            Map message = payload.message as Map

            assert payload.topic == TOPIC
            assert message.run_id == RUN_ID
            assert message.step == 'unknown'
            assert message.status == 'completed'
            assert message.task_tag == 'cached-task'
            assert message.exit_code == 0
            assert message.cached == true
            assert message.timestamp != null

            true
        }, true) >> CompletableFuture.completedFuture(socket)
    }

    def 'does not send process start event when disabled'() {
        given:
        def observer = createObserver(
                true,
                false,
                true,
                true
        )
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        when:
        observer.onProcessStart(null, null)

        then:
        0 * socket.sendText(_, _)
    }

    def 'does not send process complete event when disabled'() {
        given:
        def observer = createObserver(
                true,
                true,
                false,
                true
        )
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        when:
        observer.onProcessComplete(null, null)

        then:
        0 * socket.sendText(_, _)
    }

    def 'does not send cached event when caching events are disabled'() {
        given:
        def observer = createObserver(
                true,
                true,
                true,
                false
        )
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        when:
        observer.onProcessCached(null, null)

        then:
        0 * socket.sendText(_, _)
    }

    def 'sends workflow completed event and closes the socket'() {
        given:
        def observer = createObserver()
        def socket = Mock(WebSocket)
        setField(observer, 'mainSocket', socket)

        when:
        observer.onFlowComplete()

        then:
        1 * socket.sendText({
            matchesPayload(it as CharSequence, [
                step  : 'workflow',
                status: 'completed'
            ])
        }, true) >> CompletableFuture.completedFuture(socket)

        1 * socket.sendClose(
                WebSocket.NORMAL_CLOSURE,
                'workflow finished'
        ) >> CompletableFuture.completedFuture(socket)
    }

    def 'drops event without throwing when no socket is available'() {
        given:
        def observer = createObserver()

        when:
        observer.onProcessStart(null, null)

        then:
        noExceptionThrown()
    }
}