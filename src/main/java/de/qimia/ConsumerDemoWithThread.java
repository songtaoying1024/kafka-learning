package de.qimia;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        ConsumerDemoWithThread HH=new ConsumerDemoWithThread();
        HH.run();
    }
    public ConsumerDemoWithThread(){};

    public void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String boostrapServer="127.0.0.1:9092";
        String groupID="my-sixth-application";
        String topic="first_topic";
        //latch for dealing with multiple threads
        CountDownLatch countDownLatch=new CountDownLatch(1);
        logger.info("Creating the consumer thread");
        Runnable myConsumerThread=new ConsumerThread(countDownLatch,boostrapServer,groupID,topic);
        //start the thread
        Thread myThread=new Thread(myConsumerThread);
        myThread.start();
        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                countDownLatch.await();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        try {
            countDownLatch.await();
        }catch (InterruptedException e){
            logger.info("Application got interrupted",e);
        }finally {
            logger.info("Application is closing");
        }
    }
    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        public ConsumerThread(CountDownLatch latch,
                              String boostrapServer,
                              String groupID,
                              String topic) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    boostrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer(properties);
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); //
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }catch (WakeupException e){
                logger.info("received shutdown signal!");
            }finally {
                consumer.close();
                latch.countDown();//tell our main code we are done with the consumer
            }
        }
        public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeupException
            consumer.wakeup();
        }

    }




}
