package net.anas.kafka;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.errors.TopicExistsException;

public class KafkaTopicCreator {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java KafkaTopicCreator <topic-name> [partitions] [replication-factor]");
            System.exit(1);
        }

        String topicName = args[0];
        int partitions = args.length >= 2 ? Integer.parseInt(args[1]) : 1;
        short replicationFactor = args.length >= 3 ? Short.parseShort(args[2]) : 1;

        // Configuration du client Admin Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(props)) {
            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
            CreateTopicsResult result = adminClient.createTopics(Collections.singleton(newTopic));

            // Attente de la création du topic
            result.all().get();
            System.out.println("Topic '" + topicName + "' créé avec succès !");
        } catch (Exception e) {
            // Vérifie si c'est une exception TopicExistsException
            if (e.getCause() instanceof TopicExistsException) {
                System.out.println("Le topic '" + topicName + "' existe déjà, aucune action nécessaire.");
            } else {
                System.err.println("Erreur lors de la création du topic : " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
