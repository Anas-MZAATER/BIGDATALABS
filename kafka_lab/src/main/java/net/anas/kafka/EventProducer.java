package net.anas.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class EventProducer {
public static void main(String[] args) throws Exception{
// Verifier que le topic est fourni comme arg
if(args.length == 0){
System.out.println("Entrer le nom du topic");
return;
}
String topicName = args[0].toString(); // lire le topicName fourni comme param

Producer<String, String> producer = new KafkaProducer<>(KafkaConfig.producerProperties());

for(int i = 0; i < 10; i++)
producer.send(new ProducerRecord<String, String>(topicName,
Integer.toString(i), Integer.toString(i)));
System.out.println("Message envoye avec succes");
producer.close();
}
}