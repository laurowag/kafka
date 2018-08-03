package br.com.teste;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumer {

	public static void main(String[] args) throws IOException {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "35.184.40.26:9092");
		props.put("group.id","test");
		props.put("enable.auto.commit","true");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("max.partition.fetch.bytes","2097152");		
		
		props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-password\";");
		props.put("security.protocol","SASL_PLAINTEXT");
		props.put("sasl.mechanism","PLAIN");
		
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		
		Schema s = ReflectData.AllowNull.get().getSchema(Cliente.class);
		ReflectDatumReader<Object> reader = new ReflectDatumReader<Object>(s);

        consumer.subscribe(Arrays.asList("test"));
        int timeouts = 0;

        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(200);
            if (records.count() == 0) {
                timeouts++;
            } else {
            	System.out.printf("Got %d records after %d timeouts\n", records.count(), timeouts);
            }

            for (ConsumerRecord<String, byte[]> record: records) {
            	Cliente cliente = (Cliente) reader.read(null,
            		                DecoderFactory.get().binaryDecoder(record.value(), null));
            	System.out.println(cliente.getNome());
            }
        }
        
	}

}
