package org.lappsgrid.askme.elastic

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import org.lappsgrid.askme.core.Configuration
import org.lappsgrid.askme.core.api.AskmeMessage
import org.lappsgrid.askme.core.api.Packet
import org.lappsgrid.askme.core.metrics.Tags
import org.lappsgrid.rabbitmq.Message
import org.lappsgrid.rabbitmq.topic.MailBox
import org.lappsgrid.rabbitmq.topic.PostOffice
import org.lappsgrid.serialization.Serializer

/**
 * Starts a thread that interacts with the post office, retrieves the message
 * from the RabbitMQ post office, determines the action that is required and
 * forwards the message that came in, but updates it, for example by adding
 * documents that resulted from a database query.
 *
 * TODO:
 * 1) Update imports to phase out eager (waiting on askme-core pom)
 * 2) Add exceptions / case statements to recv method?
 */
@CompileStatic
@Slf4j("logger")
class Main {

    static final Configuration config = new Configuration()

    final PostOffice po
    MailBox box
    GetElasticDocuments process

    final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

    Counter documentsFetched
    Counter messagesReceived
    Timer timer

    Main() {
        logger.info("Exchange : {}", config.EXCHANGE)
        logger.info("Host     : {}", config.HOST)
        logger.info("Address  : {}", config.ELASTIC_MBOX)
        try {
            po = new PostOffice(config.EXCHANGE, config.HOST)
            process = new GetElasticDocuments()
            init()
        }
        catch (Exception e) {
            logger.error("Unable to construct application.", e)
        }
    }

    void init() {
        new ClassLoaderMetrics().bindTo(registry)
        new JvmMemoryMetrics().bindTo(registry)
        new JvmGcMetrics().bindTo(registry)
        new ProcessorMetrics().bindTo(registry)
        new JvmThreadMetrics().bindTo(registry)
        documentsFetched = registry.counter("documents_fetched", "service", Tags.ELASTIC)
        messagesReceived = registry.counter("messages", "service", Tags.ELASTIC)
        timer = registry.timer("elastic_query_times")
    }

    void run() {

        Object lock = new Object()
        logger.info("Running.")
        
        box = new MailBox(config.EXCHANGE, config.ELASTIC_MBOX, config.HOST) {
        
            @Override
            void recv(String s) {
                System.out.println("Message string: " + s);
                messagesReceived.increment()
                AskmeMessage message = Serializer.parse(s, AskmeMessage)
                String command = message.getCommand()
                String id = message.getId()
                printMessage('Message received', message)

                if (command == 'EXIT' || command == 'STOP') {
                    logger.info('Received shutdown message, terminating Elastic service')
                    synchronized(lock) { lock.notify() }
                }
                else if(command == 'PING') {
                    logger.info('Received PING and bouncing back response to {}', message.route[0])
                    Message response = new Message()
                    response.id = message.id
                    response.setCommand('PONG')
                    response.setRoute(message.route)
                    logger.info('Response PONG sent to {}', response.route[0])
                    Main.this.po.send(response)
                }
                else if (command == 'CORE') {
                    logger.info("CORE command received")
                    String core = message.body?.message
                    Message response = new Message()
                    response.id = message.id
                    response.setRoute(message.route)
                    if (core != null) {
                        int n = process.changeCollection(core)
                        if (n >= 0) {
                            response.command("ok")
                                .set("numDocs", Integer.toString(n))
                        }
                        else {
                            response.command("error")
                                .set("message", "Unable to switch to core $core.")
                        }
                    }
                    else {
                        message.command('error').set('message', 'No such core.')
                    }
                    Main.this.po.send(response);
                }
                else if (command == 'METRICS') {
                    Message response = new Message()
                    response.id = message.id
                    response.setCommand('ok')
                    response.body(registry.scrape())
                    response.route = message.route
                    logger.trace('Metrics sent to {}', response.route[0])
                    Main.this.po.send(response)
                }
                else {
                    logger.info('Received Message {}', id)
                    String destination = message.route[0] ?: 'the void'
                    Packet packet = (Packet) message.body
					logger.info("Index being searched is '{}'", packet.core)
                    logger.info("Gathering elastic documents for query '{}'", packet.query.query)
                    process.answer(packet, id)
                    logger.trace("Processed query from Message {}", id)
                    if (packet.documents && packet.documents.size() > 0) {
                        documentsFetched.increment(packet.documents.size())
                    }
                    printMessage('Message sent', message)
                    Main.this.po.send(message)
                    logger.debug("Message {} with elastic documents sent to {}", id, destination)
                }
            }
        }
        synchronized(lock) { lock.wait() }
        po.close()
        box.close()
        logger.info("Elastic service terminated")
    }


    static void printMessage(String header, AskmeMessage message) {
        System.out.println(sprintf('>>> %s\n%s', header, message.toString()))
    }


    static void main(String[] args) {
        logger.info("Starting Elastic service")
        Thread.start {
            new Main().run()
        }
    }
}
