package br.com.teste;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {

	public static void main(String[] args) throws IOException {
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "35.184.40.26:9092");
		props.put("group.id","test");
		props.put("enable.auto.commit","true");
		props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("max.partition.fetch.bytes","2097152");
		
		props.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-password\";");
		props.put("security.protocol","SASL_PLAINTEXT");
		props.put("sasl.mechanism","PLAIN");
		
		System.out.println("****** Vai conectar");
		KafkaProducer<String, Object> producer = new KafkaProducer<>(props);
		System.out.println("****** Conectou");
		
		
		Schema s = ReflectData.AllowNull.get().getSchema(Cliente.class);
		ReflectDatumWriter<Object> writer = new ReflectDatumWriter<Object>(s);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		try {
			for (int i = 0; i < 10; i++) {
				Cliente cliente = new Cliente();
				cliente.setCodigo(Long.parseLong(String.valueOf(i)));
				cliente.setNome("Cliente "+i);
				out.reset();
				writer.write(cliente, EncoderFactory.get().directBinaryEncoder(out, null));
				producer.send(new ProducerRecord<String, Object>("test", "chave"+i, out.toByteArray()));
			}			
		} finally {
			producer.close();
		}

	}

}
