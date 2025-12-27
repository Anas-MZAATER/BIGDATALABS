package net.anas.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Scanner;

public class ItProducer {

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("Entrer le nom du topic");
            return;
        }

        String topicName = args[0];

        Producer<String, String> producer = new KafkaProducer<>(KafkaConfig.producerProperties());

        Scanner scanner = new Scanner(System.in);

        System.out.println("Tape ton message et appuie sur ENTER.");
        System.out.println("Écris EXIT pour quitter.\n");

        while (true) {
            System.out.print("> ");
            String message = scanner.nextLine();

            if (message.equalsIgnoreCase("EXIT")) break;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(topicName, message);

            producer.send(record, (metadata, exception) -> {
                if (exception == null)
                    System.out.println("Envoyé ✓ partition="
                            + metadata.partition() + " offset=" + metadata.offset());
                else
                    exception.printStackTrace();
            });
        }

        producer.close();
        scanner.close();

        System.out.println("Producer arrêté.");
    }
}
