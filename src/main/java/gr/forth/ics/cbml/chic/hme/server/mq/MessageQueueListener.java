package gr.forth.ics.cbml.chic.hme.server.mq;

import com.github.pgasync.Db;
import com.github.pgasync.Row;
import com.rabbitmq.client.*;
import gr.forth.ics.cbml.chic.hme.server.HmeServerConfig;
import gr.forth.ics.cbml.chic.hme.server.mq.Messages.Message;
import gr.forth.ics.cbml.chic.hme.server.utils.DbUtils;
import lombok.extern.slf4j.Slf4j;
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

    public final String AMQP_URI;
    private static final String vphhfConsumerTag = "hme-vphhf-Consumer";
    private static final String mrConsumerTag = "hme-mr-Consumer";
    private static final String vphhfExchangeName = "vphhf";
    private static final String mrExchangeName = "mr";

    private final Db database;

    private final ConnectionFactory factory;
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
        factory.setAutomaticRecoveryEnabled(true); // Reconnect if connection lost..
        factory.setRequestedHeartbeat(60); // 1 min
        factory.setConnectionTimeout(5000); // 5 secs

        this.publisher = new Observables();

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
                        .whenComplete(((msg, throwable) -> {
                            boolean failed = throwable != null;
                            // When the Event/Message has been safely handled
                            // acknowledge its receipt...
                            basicAck(failed, deliveryTag, this.getChannel());
                            if (failed)
                                log.info("storing event in DB", throwable);
                            else
                                this.publisher.publish(msg);
                        }));
            else {
                log.info("Unknown message type! Routing key='{}', body='{}'", routingKey, message);
                basicAck(true, deliveryTag, this.getChannel()); // Acknowledge receipt?!
            }

        }
        private CompletableFuture<Message> saveMessage(Messages.Message msg)
        {

            final String aggregate_type = msg.toExecutionStatus().isPresent() ? "experiment" : "model";
            final UUID uuid = msg.toExecutionStatus().isPresent() ? msg.toExecutionStatus().get().getWorkflowUUID()
                    : msg.toModelsChange().get().getModelUUID();
            final String event_type = msg.eventType();
            final String jsonData = msg.toJson().toJSONString();

            return DbUtils.queryDb(db,
                    "INSERT INTO events(event_type,aggregate_type,aggregate_uuid,data)" +
                            " VALUES($1,$2,$3,$4) RETURNING event_id",
                    Arrays.asList(event_type, aggregate_type, uuid, jsonData))
                    .thenApply(resultSet -> {
                        final Row row = resultSet.row(0);
                        final Long eventId = row.getLong("event_id");
                        msg.setId(eventId);
                        return msg;
                    });

        }

    }

    public Observables observables() {
        return this.publisher;
    }

    public boolean connect(boolean persistent) {
        try {
            log.info("Trying to connect to {}, persistent? {}", AMQP_URI, persistent);

            this.conn = factory.newConnection();

            this.conn.addShutdownListener(e ->
                    log.info("MQ - SHUTDOWN initiated by app? = {} is hard error? = {} Reason: {}",
                            e.isInitiatedByApplication(), e.isHardError(),
                            e.getReason().protocolMethodName()));

            final Channel channel = conn.createChannel();

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



    private static void basicAck(boolean accepted, long deliveryTag, final Channel channel) {
        try {
            channel.basicAck(deliveryTag, accepted);
        } catch (IOException e) {
            log.info("basicAck",e);
        }
    }

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
