package solutions.Infy.kafka.connect.sendgrid.sink;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestKafkaProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"vpc-kafka-u7g406afq02.csp.aliyuncs.com:9095");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TestProducer");
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        props.put(ProducerConfig.SECURITY_PROVIDERS_CONFIG, "SASL_SSL");
        //props.put(ProducerConfig., props)

        props.put("sasl.mechanism", "PLAIN");
        //props.put("ssl.endpoint.identification.algorithm","none");
        props.put("sasl_jaas_config","org.apache.kafka.common.security.plain.PlainLoginModule required username='clp-kafka-developer01' password='clpConfluent2024@25';");
        props.put("ssl.truststore.location", "../resources/alikafka_confluent_intl-sg-u7g406afq02__public_server_cert_63.pems");
        props.put("ssl.truststore.password", "changeit");
        System.out.println("Properties are set");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record = new ProducerRecord<>("testtopic", "Test Message");
        System.out.println("REcords and topic are set");
        producer.send(record);
        System.out.println("Records are sent"+record.partition());
        System.out.println(" topic -"+record.topic());
        producer.flush();
        producer.close();
    }
}