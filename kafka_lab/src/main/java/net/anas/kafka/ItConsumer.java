package net.anas.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ItConsumer {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Usage: InteractiveConsumer <topic> <groupId>");
            System.exit(1);
        }

        String topic = args[0];
        String groupId = args[1];

        Properties props = KafkaConfig.consumerProperties(groupId);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(topic));

            System.out.println("Listening on topic: " + topic);
            System.out.println("GroupId: " + groupId);
            System.out.println("Ctrl+C to exit\n");

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "✔ key=%s | value=%s | partition=%d | offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset()
                    );
                }
            }
        }
    }
}
