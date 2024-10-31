import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.core.util.IterableStream;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

public class ServiceBusReceiverTest {

    public static void main(String[] args) {
        // credentials configured via env: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID, AZURE_SUBSCRIPTION_ID
        ServiceBusClientBuilder clientBuilder = new ServiceBusClientBuilder()
                .fullyQualifiedNamespace(getFullyQualifiedNamespace())
                .credential(new DefaultAzureCredentialBuilder().build())
                .retryOptions(getRetryOptions());

        int i = 0;
        while (i < 10000) {
            try (ServiceBusSessionReceiverClient client = clientBuilder.sessionReceiver()
                    .queueName(getQueueName())
                    .buildClient()) {
                try (ServiceBusReceiverClient receiverClient = client.acceptNextSession()) {
                    final IterableStream<ServiceBusReceivedMessage> stream = receiverClient.receiveMessages(5, Duration.ofSeconds(5));
                    final List<ServiceBusReceivedMessage> messages = stream.stream().collect(Collectors.toList());
                    System.out.println("session:" + receiverClient.getSessionId() + " messages:" + messages.size());
                }
            } catch (Exception e) {
                System.out.println("Failed to obtain session receiver: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }

            i++;
        }
    }

    private static AmqpRetryOptions getRetryOptions() {
        AmqpRetryOptions options = new AmqpRetryOptions();
        options.setMaxRetries(1);
        options.setDelay(Duration.ofMillis(250));
        options.setMaxDelay(Duration.ofMillis(1000));
        options.setTryTimeout(Duration.ofMillis(5000));
        return options;
    }

    private static String getFullyQualifiedNamespace() {
        return getServicebusNamespace() + ".servicebus.windows.net";
    }

    private static String getServicebusNamespace() {
        return System.getenv("SERVICEBUS_NAMESPACE");
    }

    private static String getQueueName() {
        return System.getenv("QUEUE_NAME");
    }
}
