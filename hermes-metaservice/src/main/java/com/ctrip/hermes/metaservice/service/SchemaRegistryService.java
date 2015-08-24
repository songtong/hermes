package com.ctrip.hermes.metaservice.service;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.metaservice.schemaregistry.SchemaKey;
import com.ctrip.hermes.metaservice.schemaregistry.SchemaValue;

@Named
public class SchemaRegistryService {

	@Inject
	private PortalMetaService metaService;
	
	@Inject
	private SchemaService schemaService;

	public boolean updateSchemaInKafkaStorage(String topic, SchemaKey schemaKey, SchemaValue schemaValue)
	      throws InterruptedException, ExecutionException {
		Properties configs = new Properties();
		configs.put("bootstrap.servers", metaService.getKafkaBrokerList());
		configs.put("client.id", "SchemaRegistryService");
		configs.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
		configs.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(configs);
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, JSON.toJSONBytes(schemaKey),
		      JSON.toJSONBytes(schemaValue));
		Future<RecordMetadata> sendFuture = producer.send(record);
		RecordMetadata recordMetadata = sendFuture.get();
		producer.close();
		if (recordMetadata.partition() >= 0)
			return true;
		else
			return false;
	}

	public SchemaValue fetchSchemaFromMetaServer(Integer schemaId) throws DalException {
		SchemaView schemaView = schemaService.getSchemaViewByAvroid(schemaId);
		if (schemaView == null) {
			return null;
		}
		SchemaValue schemaValue = new SchemaValue();
		schemaValue.setId(schemaId);
		schemaValue.setSubject(schemaView.getName());
		schemaValue.setVersion(schemaView.getVersion());
		schemaValue.setSchema(schemaView.getSchemaPreview());
		return schemaValue;
	}
}
