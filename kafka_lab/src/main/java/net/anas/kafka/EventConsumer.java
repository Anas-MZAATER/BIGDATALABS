package net.anas.kafka;

import java.util.Properties;
import java.util.Arrays;
import java.time.Duration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
public class EventConsumer {
public static void main(String[] args) throws Exception {
if(args.length == 0){
System.out.println("Entrer le nom du topic");
return;
}
String topicName = args[0].toString();

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfig.consumerProperties("test"));


consumer.subscribe(Arrays.asList(topicName));
// Afficher le nom du topic
System.out.println("Souscris au topic " + topicName);
int i = 0;
while (true) {
ConsumerRecords<String, String> records =
consumer.poll(Duration.ofMillis(100));
for (ConsumerRecord<String, String> record : records)
// Afficher l'offset, clef et valeur des enregistrements du consommateur
System.out.printf("offset = %d, key = %s, value = %s\n",
record.offset(), record.key(), record.value());
}
}
}
