package com.ctrip.hermes.rest.resource;

import java.util.Properties;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.rest.schemaregistry.SchemaKey;
import com.ctrip.hermes.rest.schemaregistry.SchemaRegistryKeyType;
import com.ctrip.hermes.rest.schemaregistry.SchemaValue;

@Path("/schemaregistry/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryResource {

	@Path("{subject}")
	@POST
	public void updateSchema(@PathParam("subject") String subject, @QueryParam("version") Integer version,
	      String schema, @QueryParam("servers") String servers, @QueryParam("schemaTopic") String topic) {
		SchemaKey schemaKey = new SchemaKey();
		schemaKey.setSubject(subject);
		schemaKey.setVersion(version);
		schemaKey.setMagic(0);
		schemaKey.setKeytype(SchemaRegistryKeyType.SCHEMA);

		SchemaValue schemaValue = JSON.parseObject(schema, SchemaValue.class);

		Properties configs = new Properties();
		configs.put("bootstrap.servers", servers);
		configs.put("client.id", "SchemaRegistryRecover");
		configs.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
		configs.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(configs);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, JSON.toJSONBytes(schemaKey),
		      JSON.toJSONBytes(schemaValue));
		producer.send(record);
		producer.close();
	}
}
