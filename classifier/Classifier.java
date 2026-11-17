package classifier;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/* Consome tweets de "tweets.raw, identifica o tópico e publica em "tweets.topics" usando o tópico como routing key. */
public class Classifier {
    private static final String INPUT_QUEUE = "tweets.raw";
    private static final String TOPICS = "tweets.topics";

    public static void main(String[] args) throws Exception {
        String url = System.getenv("AMPQ_URL");
        if (url == null || url.isEmpty()) {
            url = "amqp://tweets:tweets@localhost:5672/%2F";
        }

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(url);

        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        /* Como cada classificador recebe um tweet por vez, a fila de tweets é divida entre múltiplos classificadores. */
        channel.basicQos(1);

        final TopicMatcher topicMatcher = new TopicMatcher();

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override 
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                long deliveryTag = envelope.getDeliveryTag();

                try {
                    String json = new String(body, StandardCharsets.UTF_8);
                    JsonObject tweet = JsonParser.parseString(json).getAsJsonObject();

                    /* Tweet em branco não é classificado. */
                    if (!tweet.has("text") || tweet.get("text").isJsonNull()) {
                        System.out.println("Tweet descartado (conteúdo vazio): " + json);
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                    String text = tweet.get("text").getAsString();
                    String topic = topicMatcher.classify(text);

                    if (topic == null) {
                        System.out.println("Tweet descartado (sem tópico): " + text);
                        channel.basicAck(deliveryTag, false);
                        return;
                    }

                    /* (Re)publica o tweet com o "topic". */
                    tweet.addProperty("topic", topic);
                    byte[] newBody = tweet.toString().getBytes(StandardCharsets.UTF_8);
                    channel.basicPublish(TOPICS, topic, MessageProperties.PERSISTENT_TEXT_PLAIN, newBody);

                    channel.basicAck(deliveryTag, false);
                    System.out.println("Tweet publicado em '" + topic + "': " + text);

                } catch (RuntimeException error) {
                    System.out.println("Mensagem inválida. " + error.getMessage());
                    channel.basicNack(deliveryTag, false, false);
                }
            }
        };

        channel.basicConsume(INPUT_QUEUE, false, consumer);
        System.out.println("Classificador aguardando tweets em '" + INPUT_QUEUE + "'...");
    }
}
