package de.qimia;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String boostrapServer="127.0.0.1:9092";
        String groupID="my-fourth-application";
        String topic="first_topic";
        //create consumer configs
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,boostrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        //earliest means u want to read from the very beginning
        //latest means u want to read only the new messages

        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer(properties);

        //subscribe consumer to our object
        consumer.subscribe(Arrays.asList(topic));
        //poll for new data
        while (true){
            ConsumerRecords<String,String> records=
                    consumer.poll(Duration.ofMillis(100)); //
            for(ConsumerRecord<String,String> record:records){
                logger.info("Key: "+record.key()+", value: "+record.value());
                logger.info("Partition: "+record.partition()+ ", Offset: "+record.offset());
            }
        }
    }
}
