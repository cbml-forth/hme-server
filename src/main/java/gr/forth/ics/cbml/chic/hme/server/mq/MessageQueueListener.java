package gr.forth.ics.cbml.chic.hme.server.mq;

import com.github.pgasync.Db;
import com.github.pgasync.Row;
import com.rabbitmq.client.*;
import gr.forth.ics.cbml.chic.hme.server.HmeServerConfig;
import gr.forth.ics.cbml.chic.hme.server.mq.Messages.Message;
import gr.forth.ics.cbml.chic.hme.server.utils.DbUtils;
import lombok.extern.slf4j.Slf4j;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import net.jodah.lyra.config.RecoveryPolicy;
import net.jodah.lyra.config.RetryPolicy;
import net.jodah.lyra.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class MessageQueueListener implements AutoCloseable{
    private static final Logger log =
            LoggerFactory.getLogger(MessageQueueListener.class);

    private final Config haConfig;
    public final String AMQP_URI;
    private static final String vphhfConsumerTag = "hme-vphhf-Consumer";
    private static final String mrConsumerTag = "hme-mr-Consumer";
    private static final String vphhfExchangeName = "vphhf";
    private static final String mrExchangeName = "mr";

    private static final int prefetchCount = 20;

    private final Db database;

    private final ConnectionFactory factory;
    private final ConnectionOptions connectionOptions;
    private Connection conn;
    private final Observables publisher;


    public MessageQueueListener(HmeServerConfig config, Db db) {
        this.database = db;

        factory =  new ConnectionFactory();
        factory.setUsername(config.amqpUser());
        factory.setPassword(config.amqpPassword());
        factory.setVirtualHost(config.amqpVirtualHost());
        factory.setHost(config.amqpHost());
        factory.setPort(config.amqpPort());

        ExecutorService es = Executors.newFixedThreadPool(config.amqpThreadsNbr());
        factory.setSharedExecutor(es);
        /*
        factory.setAutomaticRecoveryEnabled(true); // Reconnect if connection lost..
        factory.setRequestedHeartbeat(60); // 1 min
        factory.setConnectionTimeout(5000); // 5 secs
        */

        this.haConfig = new Config()
                .withRecoveryPolicy(new RecoveryPolicy()
                        .withBackoff(Duration.seconds(1), Duration.seconds(30))
                        .withMaxAttempts(-1))
                .withRetryPolicy(new RetryPolicy()
                        .withMaxAttempts(10)
                        .withInterval(Duration.seconds(1))
                        .withMaxDuration(Duration.minutes(5)));
        this.connectionOptions = new ConnectionOptions().withConnectionFactory(factory);


        this.publisher = new Observables(db);

        this.AMQP_URI = String.format("amqp://%s:%s@%s:%d/%s",
                config.amqpUser(),
                config.amqpPassword(),
                config.amqpHost(),
                config.amqpPort(),
                config.amqpVirtualHost());
    }


    @Slf4j
    static class Consumer extends DefaultConsumer {
        private final Observables publisher;
        private final Db db;

        public Consumer(Observables publisher, Db db, Channel channel) {
            super(channel);
            this.publisher = publisher;
            this.db = db;
        }
        @Override
        public void handleDelivery(String consumerTag,
                                   Envelope envelope,
                                   AMQP.BasicProperties properties,
                                   byte[] body)
                throws IOException {

            final Channel myChannel = this.getChannel();

            final String routingKey = envelope.getRoutingKey();
            final String message = new String(body, StandardCharsets.UTF_8);

            long deliveryTag = envelope.getDeliveryTag();
            // log.info("======> {} : {}", routingKey, message);

            // Try to identify what kind of message this is
            Optional<Message> event = Messages.of(routingKey, message);
            // if the message is of known type, save it and publish it to any
            // subscribers
            if (event.isPresent())
                this.saveMessage(event.get())
                        .whenComplete(((msgOpt, throwable) -> {
                            boolean requeue = true;
                            boolean multipleAck = false;
                            boolean msgSavedOk = throwable == null;

                            // This can be handled in a different thread than the one of
                            // the Consumer. Is that a problem?
                            //
                            // According to
                            // http://stackoverflow.com/questions/30695375/rabbitmq-and-channels-java-thread-safety
                            // it's not:
                            //
                            // "consuming (basicConsume) and acking from more than one thread is a common
                            // rabbitmq pattern that is already used by the java client."

                            try {
                                if (msgSavedOk) {
                                    // When the Event/Message has been safely handled
                                    // (even if we don't have such Experiment in the database)
                                    // acknowledge its receipt...
                                    myChannel.basicAck(deliveryTag, multipleAck);
                                    log.info("BasicAck-ed {}", deliveryTag);
                                    if (msgOpt.isPresent())
                                        this.publisher.publish(msgOpt.get());
                                }
                                else {
                                    // We couldn't store the Event in the Database, an exception was thrown
                                    myChannel.basicNack(deliveryTag, multipleAck, requeue);
                                    log.info("storing event in DB", throwable);
                                }
                            } catch (IOException e) {
                                log.debug("basicAck", e);
                            }
                        }));
            else {
                log.info("Unknown message type! Routing key='{}', body='{}'", routingKey, message);
                myChannel.basicReject(deliveryTag, /* requeue */ false);
            }

        }
        private CompletableFuture<Optional<Message>> saveMessage(Messages.Message msg)
        {
            final boolean isExecutionMsg = msg.toExecutionStatus().isPresent();
            final String aggregate_type = isExecutionMsg ? "experiment" : "model";
            final UUID uuid = isExecutionMsg ? msg.toExecutionStatus().get().getWorkflowUUID()
                    : msg.toModelsChange().get().getModelUUID();
            final String event_type = msg.eventType();
            final String jsonData = msg.toJson().toJSONString();

            final String sql = isExecutionMsg ?
                    "INSERT INTO events(event_type,aggregate_type,aggregate_uuid,data)" +
                            " SELECT $1,$2,experiment_uid, $4 FROM experiments WHERE workflow_uuid=$3" +
                            " RETURNING event_id"
                    : "INSERT INTO events(event_type,aggregate_type,aggregate_uuid,data)" +
                    " VALUES($1,$2,$3,$4) RETURNING event_id";
            return DbUtils.queryDb(db, sql,
                    Arrays.asList(event_type, aggregate_type, uuid, jsonData))
                    .thenApply(resultSet -> {
                        // It can be the case (in test cases) that we receive a Execution message
                        // for an experiment that we don't have in our database. In this case,
                        // we don't get any rows back..
                        if (resultSet.size() > 0) {
                            final Row row = resultSet.row(0);
                            final long eventId = row.getLong(0);
                            msg.setId(eventId);
                            return Optional.of(msg);
                        }
                        // Return an "empty" value if there's no such experiment in
                        // our database:
                        return Optional.empty();
                    });

        }

    }

    public Observables observables() {
        return this.publisher;
    }

    public boolean connect(boolean persistent) {
        try {
            log.info("Trying to connect to {}, persistent? {}", AMQP_URI, persistent);

            this.conn = Connections.create(this.connectionOptions, this.haConfig);

            this.conn.addShutdownListener(e ->
                    log.info("MQ - SHUTDOWN initiated by app? = {} is hard error? = {} Reason: {}",
                            e.isInitiatedByApplication(), e.isHardError(),
                            e.getReason().protocolMethodName()));

            final Channel channel = conn.createChannel();
            // Specify the maximum number of messages that the server will deliver
            // before requiring acknowledgements. This allow us to have control on the
            // concurrency (the maximum number of concurrent messages that the RabbitMQ server
            // will send us) and therefore it's a basic support for 'back-pressure'.
            channel.basicQos(prefetchCount);

            channel.exchangeDeclare(vphhfExchangeName, "topic", true);
            channel.exchangeDeclare(mrExchangeName, "topic", true);
            String vphhfQueueName = "hme-vphhf-mailbox";
            String mrQueueName = "hme-mr-mailbox";
            if (persistent) {
                final boolean durable = true;
                final boolean exclusive = false;
                final boolean autoDelete = false;
                channel.queueDeclare(vphhfQueueName, durable, exclusive, autoDelete, null);
                channel.queueDeclare(mrQueueName, durable, exclusive, autoDelete, null);
            } else {
                vphhfQueueName = channel.queueDeclare().getQueue();
                mrQueueName = channel.queueDeclare().getQueue();
            }

            channel.queueBind(vphhfQueueName, vphhfExchangeName, "#"); // All keys...
            channel.queueBind(mrQueueName, mrExchangeName, "#"); // All keys...

            final boolean autoAck = false;
            Consumer consumer = new Consumer(this.publisher, this.database, channel);
            channel.basicConsume(vphhfQueueName, autoAck, vphhfConsumerTag, consumer);
            channel.basicConsume(mrQueueName, autoAck, mrConsumerTag, consumer);

            log.info("[AMQP] Connected to {} vph queue= {} and mr queue={}", AMQP_URI, vphhfQueueName, mrQueueName);

        } catch (Exception e) {

            log.info("connect", e);
            return false;

        }


        return true;
    }


        /*
    void handleEvent(final Message message, long deliveryTag, final Channel channel) {


        final Matcher matcher = experimentStatusPattern.matcher(message.subject);
        if (matcher.matches()) {
            try {
                final UUID workflowUuid = UUID.fromString(matcher.group(1));
                final EXP_RUN_STATE status = EXP_RUN_STATE.fromString(message.message);
                System.err.println("\t\t\t--> " + workflowUuid + " : " + status);
                db.updateWorkflowStatusObs(workflowUuid, status.toString())
                        .doOnError(ex -> basicAck(false, deliveryTag, channel))
                        .doOnCompleted(() -> basicAck(true, deliveryTag, channel))
                        .filter(Objects::nonNull) // Maybe the Experiment was not found in the db
                        .flatMap(db::getExperimentObs) // Retrieve the Experiment for the Observers
                        .map(Experiment::toJson)
                        .observeOn(scheduler)
                        .subscribe( // Notify all the 'observers' (per user)
                                jsonObject -> {
                                    // System.err.println("CREATE EXPERIMENT EVT ON " + Thread.currentThread().getName());
                                    final String user_id = jsonObject.getAsString("user_uid");
                                    final UUID exp_uuid = UUID.fromString(jsonObject.getAsString("uuid"));
                                    final ExperimentStatusEvent statusEvent = new ExperimentStatusEvent(user_id, exp_uuid, status);
                                    statusEvent.setJson(jsonObject);
                                    getOrCreateUserSubject(user_id).onNext(statusEvent);
                                },
                                Throwable::printStackTrace);
                return;
            } catch (Throwable ex) {
                ex.printStackTrace();
                basicAck(false, deliveryTag, channel);
            }
            return;
        }

        final Matcher modelsMatcher = modelsStatusPattern.matcher(message.subject);
        if (modelsMatcher.matches()) {
            val evt = modelsMatcher.group(1);
            System.err.println("--> [MR: " + evt+ "]: "+ message.getMessage());
            basicAck(true, deliveryTag, channel);
            return;
        }
    }
        */


    @Override
    public void close() {
        try {
            this.publisher.close();
            if (this.conn != null) {
                this.conn.close();
            }
        } catch (Exception e) {
        }
    }
}
