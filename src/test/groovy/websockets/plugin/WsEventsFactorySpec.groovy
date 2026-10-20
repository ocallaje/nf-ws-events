package websockets.plugin

import nextflow.Session
import spock.lang.Specification

class WsEventsFactorySpec extends Specification {

    def 'returns empty collection when configuration is missing'() {
        given:
        def factory = new WsEventsFactory()
        def session = Stub(Session) {
            getConfig() >> [:]
        }

        when:
        def result = factory.create(session)

        then:
        result.isEmpty()
    }

    def 'returns empty collection when disabled'() {
        given:
        def factory = new WsEventsFactory()
        def session = Stub(Session) {
            getConfig() >> [
                wsEvents: [
                    enabled: false,
                    url: 'ws://localhost:7777'
                ]
            ]
        }

        when:
        def result = factory.create(session)

        then:
        result.isEmpty()
    }

    def 'creates observer when enabled and URL is configured'() {
        given:
        def factory = new WsEventsFactory()
        def session = Stub(Session) {
            getConfig() >> [
                wsEvents: [
                    enabled: true,
                    url: 'ws://localhost:7777'
                ]
            ]
        }

        when:
        def result = factory.create(session)

        then:
        result.size() == 1
        result.first() instanceof WsEventsObserver
    }

    def 'throws when enabled but no URL is configured'() {
        given:
        def factory = new WsEventsFactory()
        def session = Stub(Session) {
            getConfig() >> [
                wsEvents: [
                    enabled: true
                ]
            ]
        }

        when:
        factory.create(session)

        then:
        thrown(IllegalArgumentException)
    }

    def 'allows registration URL without WebSocket URL'() {
        given:
        def factory = new WsEventsFactory()
        def session = Stub(Session) {
            getConfig() >> [
                wsEvents: [
                    enabled: true,
                    url_register: 'http://localhost:7777/register'
                ]
            ]
        }

        when:
        def result = factory.create(session)

        then:
        result.size() == 1
        result.first() instanceof WsEventsObserver
    }
}