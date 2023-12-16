package io.moquette.integration.mqtt5;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.moquette.broker.Server;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.DeclarativeAuthorizatorPolicy;
import io.moquette.broker.security.IAuthorizatorPolicy;
import io.moquette.broker.subscriptions.Topic;
import io.moquette.integration.IntegrationUtils;
import io.moquette.testclient.Client;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttReasonCodeAndPropertiesVariableHeader;
import io.netty.handler.codec.mqtt.MqttReasonCodes;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.moquette.integration.mqtt5.ConnectTest.assertConnectionAccepted;
import static io.moquette.integration.mqtt5.ConnectTest.verifyNoPublish;
import static org.junit.jupiter.api.Assertions.*;

public class SharedSubscriptionTest extends AbstractServerIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(SharedSubscriptionTest.class);

    @Override
    public String clientName() {
        return "subscriber";
    }

    @Test
    public void givenAClientSendingBadlyFormattedSharedSubscriptionNameThenItIsDisconnected() {
        connectLowLevel();

        MqttMessage received = lowLevelClient.subscribeWithError("$share/+/measures/temp", MqttQoS.AT_LEAST_ONCE);

        // verify received is a disconnect with an error
        verifyOfType(received, MqttMessageType.DISCONNECT);
        MqttReasonCodeAndPropertiesVariableHeader disconnectHeader = (MqttReasonCodeAndPropertiesVariableHeader) received.variableHeader();
        assertEquals(MqttReasonCodes.Disconnect.MALFORMED_PACKET.byteValue(), disconnectHeader.reasonCode());
    }

    private static void verifyOfType(MqttMessage received, MqttMessageType mqttMessageType) {
        assertEquals(mqttMessageType, received.fixedHeader().messageType());
    }

    private void connectLowLevel() {
        MqttConnAckMessage connAck = lowLevelClient.connectV5();
        assertConnectionAccepted(connAck, "Connection must be accepted");
    }

    @Test
    public void givenClientSubscribingToSharedTopicThenReceiveTheExpectedSubscriptionACK() {
        connectLowLevel();

        MqttMessage received = lowLevelClient.subscribeWithError("$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        verifyOfType(received, MqttMessageType.SUBACK);
        MqttSubAckMessage subAckMessage = (MqttSubAckMessage) received;
        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(), "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.AT_LEAST_ONCE.value(), grantedQoSes.iterator().next());
    }

    @Test
    public void givenATopicNotReadableWhenAClientSubscribeSharedThenReceiveSubackWithNegativeResponse() throws IOException {
        // stop already started broker instance
        stopServer();

        final IAuthorizatorPolicy policy = new DeclarativeAuthorizatorPolicy.Builder().build();
        startServer(dbPath, policy);

        // Connect the client to newly started broker
        lowLevelClient = new Client("localhost").clientId(clientName());

        connectLowLevel();

        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe("$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.FAILURE.value(), grantedQoSes.iterator().next(),
            "Not readable topic should reflect also in shared subscription");
    }


    @Test
    public void givenClientSubscribingToSharedAndNonSharedWhenTheSharedIsNotReadableReceivesPositiveAckOnlyForNonShared() throws IOException {
        // stop already started broker instance
        stopServer();

        final String clientId = clientName();
        final IAuthorizatorPolicy policy = new DeclarativeAuthorizatorPolicy.Builder()
            .readFrom(Topic.asTopic("/sensors/living/temp"), null, clientId)
            .build();
        startServer(dbPath, policy);

        // Connect the client to newly started broker
        lowLevelClient = new Client("localhost").clientId(clientId);

        connectLowLevel();

        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe(
            "/sensors/living/temp", MqttQoS.EXACTLY_ONCE,
            "$share/metrics/measures/temp", MqttQoS.AT_LEAST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(2, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        Iterator<Integer> replyQoSes = grantedQoSes.iterator();
        assertEquals(MqttQoS.EXACTLY_ONCE.value(), replyQoSes.next(),
            "Non shared readable subscription must be accepted");
        assertEquals(MqttQoS.FAILURE.value(), replyQoSes.next(),
            "Not readable topic should reflect also in shared subscription");
    }

    protected void startServer(String dbPath, IAuthorizatorPolicy authPolicy) throws IOException {
        broker = new Server();
        final Properties configProps = IntegrationUtils.prepareTestProperties(dbPath);
        config = new MemoryConfig(configProps);
        broker.startServer(config, null, null, null, authPolicy);
    }

    @Test
    public void givenASharedSubscriptionClientReceivesANotification() throws Exception {
        final Mqtt5BlockingClient subscriberClient = createSubscriberClient();
        subscriberClient.subscribeWith()
            .topicFilter("$share/collectors/metric/temperature/#")
            .send();

        Mqtt5BlockingClient publisherClient = createPublisherClient();

        verifyPublishedMessage(subscriberClient, unused -> publisherClient.publishWith()
            .topic("metric/temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .send(), MqttQos.AT_MOST_ONCE, "18", "Shared message must be received", 10);
    }

    @Test
    public void givenAClientWithOverlappingSharedSubscriptionsThenReceivesMultiplePublishes() throws InterruptedException {
        // Connect a subscriber client
        lowLevelClient = new Client("localhost").clientId(clientName());

        connectLowLevel();

        // subscribe to a shared topic
        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe(
            "$share/collectors/metric/temperature/#", MqttQoS.AT_MOST_ONCE,
            "$share/thermo_living/metric/temperature/living", MqttQoS.AT_MOST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(2, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.AT_MOST_ONCE.value(), grantedQoSes.iterator().next(),
            "Client is subscribed to the shared topic");
        assertEquals(MqttQoS.AT_MOST_ONCE.value(), grantedQoSes.iterator().next(),
            "Client is subscribed to the shared topic");

        Mqtt5BlockingClient publisherClient = createPublisherClient();
        publish(publisherClient, "metric/temperature/living", MqttQos.AT_MOST_ONCE);

        MqttMessage received = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyPubPayload(received, "18");
        received = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        verifyPubPayload(received, "18");
    }

    private static void verifyPubPayload(MqttMessage received, String expectedPayload) {
        assertNotNull(received);
        assertEquals(MqttPublishMessage.class, received.getClass());
        MqttPublishMessage pub = (MqttPublishMessage) received;
        String payload = pub.payload().asByteBuf().toString(StandardCharsets.UTF_8);
        assertEquals(expectedPayload, payload);
        assertTrue(pub.release(), "received message must be deallocated");
    }

    @NotNull
    private Mqtt5BlockingClient createSubscriberClient() {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier(clientName())
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect().getReasonCode(), "Subscriber connected");
        return client;
    }

    @NotNull
    private Mqtt5BlockingClient createPublisherClient() {
        final Mqtt5BlockingClient client = MqttClient.builder()
            .useMqttVersion5()
            .identifier("publisher")
            .serverHost("localhost")
            .serverPort(1883)
            .buildBlocking();
        assertEquals(Mqtt5ConnAckReasonCode.SUCCESS, client.connect().getReasonCode(), "Publisher connected");
        return client;
    }

    private static void verifyPublishedMessage(Mqtt5BlockingClient client, MqttQos expectedQos, String expectedPayload, String errorMessage, int timeoutSeconds) throws InterruptedException {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            Optional<Mqtt5Publish> publishMessage = publishes.receive(timeoutSeconds, TimeUnit.SECONDS);
            if (!publishMessage.isPresent()) {
                fail("Expected to receive a publish message");
                return;
            }
            Mqtt5Publish msgPub = publishMessage.get();
            final String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
            assertEquals(expectedPayload, payload, errorMessage);
            assertEquals(expectedQos, msgPub.getQos());
        }
    }

    private static void verifyPublishedMessage(Mqtt5BlockingClient client, Consumer<Void> action, MqttQos expectedQos,
                                               String expectedPayload, String errorMessage, int timeoutSeconds) throws Exception {
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            action.accept(null);
            Optional<Mqtt5Publish> publishMessage = publishes.receive(timeoutSeconds, TimeUnit.SECONDS);
            if (!publishMessage.isPresent()) {
                fail("Expected to receive a publish message");
                return;
            }
            Mqtt5Publish msgPub = publishMessage.get();
            final String payload = new String(msgPub.getPayloadAsBytes(), StandardCharsets.UTF_8);
            assertEquals(expectedPayload, payload, errorMessage);
            assertEquals(expectedQos, msgPub.getQos());
        }
    }

    @Test
    public void whenAClientSubscribeToASharedTopicThenDoesntReceiveAnyRetainedMessagedOnTheMatchingTopicFilter() throws InterruptedException {
        // publish a message with retained on a shared topic
        Mqtt5BlockingClient publisherClient = createPublisherClient();
        publisherClient.publishWith()
            .topic("temperature/living")
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .qos(MqttQos.AT_LEAST_ONCE) // Broker retains only QoS1 and QoS2
            .retain(true)
            .send();

        // Connect a subscriber client
        lowLevelClient = new Client("localhost").clientId(clientName());

        connectLowLevel();

        // subscribe to a shared topic
        MqttSubAckMessage subAckMessage = lowLevelClient.subscribe(
            "$share/collectors/temperature/#", MqttQoS.AT_LEAST_ONCE);

        List<Integer> grantedQoSes = subAckMessage.payload().grantedQoSLevels();
        assertEquals(1, grantedQoSes.size(),
            "Granted qos list must be the same cardinality of the subscribe request");
        assertEquals(MqttQoS.AT_LEAST_ONCE.value(), grantedQoSes.iterator().next(),
            "Client is subscribed to the shared topic");

        MqttMessage received = lowLevelClient.receiveNextMessage(Duration.ofSeconds(1));
        assertNull(received, "No retained messages MUST be published");
    }

    @Test
    public void givenSharedSubscriptionWithCertainQoSWhenSameClientWithSameShareSubscribeToSameTopicFilterThenQoSUpdates() throws Exception {
        final Mqtt5BlockingClient subscriberClient = createSubscriberClient();
        subscribe(subscriberClient, "$share/collectors/metric/temperature/living", MqttQos.AT_MOST_ONCE);

        Mqtt5BlockingClient publisherClient = createPublisherClient();

        // because the PUB is at QoS1 and the subscription is at QoS0, the subscribed doesn't receive any message
        verifyPublishedMessage(subscriberClient,
            unused -> publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE),
            MqttQos.AT_MOST_ONCE, "18", "QoS0 publish message is expected by the subscriber when subscribed with AT_MOST_ONCE", 1);

        LOG.info("Before repeating with AT_LEAST_ONCE");

        // update QoS for shared subscription
        subscribe(subscriberClient, "$share/collectors/metric/temperature/living", MqttQos.AT_LEAST_ONCE);

        // This time the publish reaches the subscription
        verifyPublishedMessage(subscriberClient, v -> {
            // publish the message again and verify the captured message
            publish(publisherClient, "metric/temperature/living", MqttQos.AT_LEAST_ONCE);
        }, MqttQos.AT_LEAST_ONCE, "18", "Shared message must be received", 30);
    }

    private static void publish(Mqtt5BlockingClient publisherClient, String topicName, MqttQos mqttQos) {
        publisherClient.publishWith()
            .topic(topicName)
            .qos(mqttQos)
            .payload("18".getBytes(StandardCharsets.UTF_8))
            .send();
    }

    private static void subscribe(Mqtt5BlockingClient subscriberClient, String topicFilter, MqttQos mqttQos) {
        subscriberClient.subscribeWith()
            .topicFilter(topicFilter)
            .qos(mqttQos)
            .send();
    }
}