package com.ctrip.hermes.metaservice.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.Schema.Parser;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.lookup.util.StringUtils;

import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.model.Schema;
import com.ctrip.hermes.metaservice.model.SchemaDao;
import com.ctrip.hermes.metaservice.model.SchemaEntity;

@Named
public class SchemaService {

	private static final Logger m_logger = LoggerFactory.getLogger(SchemaService.class);

	private SchemaRegistryClient avroSchemaRegistry;

	@Inject
	private SchemaDao m_schemaDao;

	@Inject
	private PortalMetaService m_metaService;

	@Inject
	private CompileService m_compileService;

	/**
	 * 
	 * @param schemaName
	 * @param schemaContent
	 * @throws IOException
	 * @throws RestClientException
	 * @return avroid
	 */
	public int checkAvroSchema(String schemaName, byte[] schemaContent) throws IOException, RestClientException {
		Parser parser = new Parser();
		org.apache.avro.Schema avroSchema = parser.parse(new String(schemaContent));
		getAvroSchemaRegistry().updateCompatibility(schemaName, "NONE");
		int avroid = getAvroSchemaRegistry().register(schemaName, avroSchema);
		return avroid;
	}

	/**
	 * 
	 * @param topic
	 * @param avroid
	 * @return
	 * @throws DalException
	 */
	public boolean isAvroSchemaExist(Topic topic, int avroid) throws DalException {
		List<Schema> schemas = listSchemaMeta(topic);
		if (schemas == null || schemas.size() == 0) {
			return false;
		}
		for (Schema schema : schemas) {
			if (schema.getAvroid() == avroid) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * @param metaSchema
	 * @param Idl
	 * @throws ParseException
	 * @throws IOException
	 * @throws DalException
	 */
	public void compileAvro(Schema metaSchema, org.apache.avro.compiler.idl.Idl idl) throws ParseException, IOException,
	      DalException {
		final Path destDir = Files.createTempDirectory("avroschema");
		Protocol compilationUnit = idl.CompilationUnit();
		for (org.apache.avro.Schema schema : compilationUnit.getTypes()) {
			SpecificCompiler compiler = new SpecificCompiler(schema);
			compiler.compileToDestination(null, destDir.toFile());
		}
		idl.close();
		Path jarFile = Files.createTempFile(metaSchema.getName(), ".jar");
		for (org.apache.avro.Schema schema : compilationUnit.getTypes()) {
			Path compileDir = new File(destDir + schema.getNamespace().replace('.', '/')).toPath();
			m_compileService.compile(compileDir);
			m_compileService.jar(destDir, jarFile);
			m_logger.info("Compile avro {}", jarFile.getFileName());
		}

		byte[] jarContent = Files.readAllBytes(jarFile);
		metaSchema.setJarContent(jarContent);
		FormDataContentDisposition disposition = FormDataContentDisposition.name(metaSchema.getName())
		      .creationDate(new Date(System.currentTimeMillis()))
		      .fileName(metaSchema.getName() + "_" + metaSchema.getVersion() + ".jar").size(jarFile.toFile().length())
		      .build();
		metaSchema.setJarProperties(disposition.toString());
		m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
		Files.delete(jarFile);
		m_compileService.delete(destDir);
	}

	/**
	 * 
	 * @param metaSchema
	 * @param avroSchema
	 * @throws IOException
	 * @throws DalException
	 */
	public void compileAvro(Schema metaSchema, org.apache.avro.Schema avroSchema) throws IOException, DalException {
		m_logger.info(String.format("Compile %s by %s", metaSchema.getName(), avroSchema.getName()));
		final Path destDir = Files.createTempDirectory("avroschema");
		SpecificCompiler compiler = new SpecificCompiler(avroSchema);
		compiler.compileToDestination(null, destDir.toFile());

		m_compileService.compile(destDir);
		Path jarFile = Files.createTempFile(metaSchema.getName(), ".jar");
		m_compileService.jar(destDir, jarFile);

		byte[] jarContent = Files.readAllBytes(jarFile);
		metaSchema.setJarContent(jarContent);
		FormDataContentDisposition disposition = FormDataContentDisposition.name(metaSchema.getName())
		      .creationDate(new Date(System.currentTimeMillis()))
		      .fileName(metaSchema.getName() + "_" + metaSchema.getVersion() + ".jar").size(jarFile.toFile().length())
		      .build();
		metaSchema.setJarProperties(disposition.toString());
		m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
		Files.delete(jarFile);
		m_compileService.delete(destDir);
	}

	public Topic updateTopic(Topic topic) throws DalException {
		Meta meta = m_metaService.getMeta();
		meta.removeTopic(topic.getName());
		topic.setLastModifiedTime(new Date(System.currentTimeMillis()));
		meta.addTopic(topic);
		m_metaService.updateMeta(meta);
		return topic;
	}

	/**
	 * 
	 * @param schemaView
	 * @param topicId
	 * @return
	 * @throws DalException
	 * @throws RestClientException
	 * @throws IOException
	 */
	public SchemaView createSchema(SchemaView schemaView, Topic topic) throws DalException, IOException,
	      RestClientException {
		m_logger.info("Creating schema for topic: {}, schema: {}", topic.getName(), schemaView);
		Schema schema = toSchema(schemaView);
		Date now = new Date(System.currentTimeMillis());
		schema.setCreateTime(now);
		schema.setDataChangeLastTime(now);
		schema.setName(topic.getName() + "-value");
		schema.setTopicId(topic.getId());
		try {
			Schema maxVersionSchemaMeta = m_schemaDao.getMaxVersionByTopic(topic.getId(), SchemaEntity.READSET_FULL);
			schema.setVersion(maxVersionSchemaMeta.getVersion() + 1);
		} catch (Exception e) {
			schema.setVersion(1);
		}
		m_schemaDao.insert(schema);

		topic.setSchemaId(schema.getId());
		updateTopic(topic);

		if ("avro".equals(schema.getType())) {
			if (StringUtils.isNotEmpty(schema.getCompatibility())) {
				getAvroSchemaRegistry().updateCompatibility(schema.getName(), schema.getCompatibility());
			}
		}

		m_logger.info("Created schema: {}", schema);
		return toSchemaView(schema);
	}

	/**
	 * 
	 * @param id
	 * @throws DalException
	 */
	public void deleteSchema(long id) throws DalException {
		m_logger.info("Deleting schema id: {}", id);
		Schema schema = m_schemaDao.findByPK(id, SchemaEntity.READSET_FULL);
		Topic topic = m_metaService.findTopicById(schema.getTopicId());
		if (topic != null) {
			List<Schema> schemas = m_schemaDao.findByTopic(topic.getId(), SchemaEntity.READSET_FULL);
			for (Schema s : schemas) {
				if (s.getId() == id) {
					schemas.remove(s);
					break;
				}
			}
			if (schemas.size() > 0 && topic.getSchemaId() != schemas.get(0).getId()) {
				topic.setSchemaId(schemas.get(0).getId());
				updateTopic(topic);
			} else if (schemas.size() == 0) {
				topic.setSchemaId(null);
				updateTopic(topic);
			}
			m_schemaDao.deleteByPK(schema);
			m_logger.info("Deleted schema id: {}", id);
		}
	}

	/**
	 * 
	 * @param topic
	 * @throws DalException
	 */
	public void deleteSchemas(Topic topic) throws DalException {
		List<Schema> schemas = m_schemaDao.findByTopic(topic.getId(), SchemaEntity.READSET_FULL);
		for (Schema schema : schemas) {
			m_schemaDao.deleteByPK(schema);
		}
	}

	/**
	 * 
	 * @param metaSchema
	 * @param groupId
	 * @param artifactId
	 * @param version
	 * @param repositoryId
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws java.text.ParseException
	 */
	public void deployToMaven(Schema metaSchema, String groupId, String artifactId, String version, String repositoryId)
	      throws NumberFormatException, IOException, java.text.ParseException {
		m_logger.info("Deploying to maven, {}, groupId {}, artifactId {}, version {}, repositoryId {}", metaSchema,
		      groupId, artifactId, version, repositoryId);
		Path jarPath = Files.createTempFile(metaSchema.getName(), ".jar");
		com.google.common.io.Files.write(metaSchema.getJarContent(), jarPath.toFile());
		try {
			if ("snapshots".equals(repositoryId)) {
				m_compileService.deployToMaven(jarPath, groupId, artifactId, version, "snapshots");
			} else if ("releases".equals(repositoryId)) {
				m_compileService.deployToMaven(jarPath, groupId, artifactId, version, "releases");
			}
		} finally {
			m_compileService.delete(jarPath);
		}
		m_logger.info("Deployed groupId {}, artifactId {}, version {}, repositoryId {}", groupId, artifactId, version,
		      repositoryId);
	}

	private SchemaRegistryClient getAvroSchemaRegistry() throws IOException {
		if (avroSchemaRegistry == null) {
			Codec avroCodec = m_metaService.findCodecByType("avro");
			if (avroCodec == null) {
				throw new RuntimeException("Could not get the avro codec");
			}
			String schemaRegistryUrl = avroCodec.getProperties().get("schema.registry.url").getValue();
			m_logger.info("schema.registry.url:" + schemaRegistryUrl);
			avroSchemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
		}
		return avroSchemaRegistry;
	}

	/**
	 * 
	 * @param schema
	 * @return
	 * @throws IOException
	 * @throws RestClientException
	 */
	public String getCompatible(Schema schema) throws IOException, RestClientException {
		return getAvroSchemaRegistry().getCompatibility(schema.getName());
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 * @throws DalException
	 */
	public Schema getSchemaMeta(long schemaId) throws DalException {
		Schema schema = m_schemaDao.findByPK(schemaId, SchemaEntity.READSET_FULL);
		return schema;
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 * @throws DalException
	 * @throws IOException
	 * @throws RestClientException
	 */
	public SchemaView getSchemaView(long schemaId) throws DalException, IOException, RestClientException {
		Schema schema = getSchemaMeta(schemaId);
		SchemaView schemaView = toSchemaView(schema);
		return schemaView;
	}

	/**
	 * 
	 * @param avroid
	 * @return
	 * @throws DalException
	 */
	public SchemaView getSchemaViewByAvroid(int avroid) throws DalException {
		Schema schema = m_schemaDao.findByAvroid(avroid, SchemaEntity.READSET_FULL);
		SchemaView schemaView = toSchemaView(schema);
		return schemaView;
	}

	/**
	 * 
	 * @return
	 * @throws DalException
	 */
	public List<Schema> listLatestSchemaMeta() throws DalException {
		List<Schema> schemas = m_schemaDao.listLatest(SchemaEntity.READSET_FULL);
		return schemas;
	}

	/**
	 * 
	 * @return
	 * @throws DalException
	 */
	public List<SchemaView> listSchemaViews() throws DalException {
		List<Schema> schemas = m_schemaDao.list(SchemaEntity.READSET_FULL);
		List<SchemaView> result = new ArrayList<SchemaView>();
		for (Schema schema : schemas) {
			SchemaView schemaView = SchemaService.toSchemaView(schema);
			result.add(schemaView);
		}
		return result;
	}

	/**
	 * 
	 * @param topic
	 * @return
	 * @throws DalException
	 */
	public List<Schema> listSchemaMeta(Topic topic) throws DalException {
		List<Schema> schemas = m_schemaDao.findByTopic(topic.getId(), SchemaEntity.READSET_FULL);
		return schemas;
	}

	/**
	 * 
	 * @param topic
	 * @return
	 * @throws DalException
	 */
	public List<SchemaView> listSchemaView(Topic topic) throws DalException {
		List<SchemaView> result = new ArrayList<SchemaView>();
		List<Schema> schemaMetas = listSchemaMeta(topic);
		for (Schema schema : schemaMetas) {
			SchemaView schemaView = SchemaService.toSchemaView(schema);
			result.add(schemaView);
		}
		return result;
	}

	/**
	 * 
	 * @param schemaView
	 * @param fileContent
	 * @param fileHeader
	 * @return
	 * @throws IOException
	 * @throws DalException
	 * @throws RestClientException
	 */
	public SchemaView updateSchemaFile(SchemaView schemaView, byte[] fileContent, FormDataContentDisposition fileHeader)
	      throws IOException, DalException, RestClientException {
		m_logger.info("update schema {} by file {}", schemaView, fileHeader.getFileName());
		SchemaView result = null;
		if (schemaView.getType().equals("json")) {
			result = uploadJsonSchema(schemaView, null, null, fileContent, fileHeader);
		} else if (schemaView.getType().equals("avro")) {
			result = uploadAvroSchema(schemaView, fileContent, fileHeader, null, null);
		}
		return result;
	}

	/**
	 * 
	 * @param schemaView
	 * @param schemaContent
	 * @param schemaHeader
	 * @param jarContent
	 * @param jarHeader
	 * @throws IOException
	 * @throws DalException
	 * @throws RestClientException
	 */
	public SchemaView uploadAvroSchema(SchemaView schemaView, byte[] schemaContent,
	      FormDataContentDisposition schemaHeader, byte[] jarContent, FormDataContentDisposition jarHeader)
	      throws IOException, DalException, RestClientException {
		if (schemaContent == null) {
			return schemaView;
		}

		boolean isUpdated = false;
		Schema metaSchema = toSchema(schemaView);
		if (schemaContent != null) {
			metaSchema.setSchemaContent(schemaContent);
			metaSchema.setSchemaProperties(schemaHeader.toString());

			if (schemaHeader.getFileName().endsWith("avsc")) {
				Parser parser = new Parser();
				org.apache.avro.Schema avroSchema = parser.parse(new String(schemaContent));
				int avroid = getAvroSchemaRegistry().register(metaSchema.getName(), avroSchema);
				metaSchema.setAvroid(avroid);

				compileAvro(metaSchema, avroSchema);
			}
			isUpdated = true;
		}
		if (jarContent != null) {
			metaSchema.setJarContent(jarContent);
			metaSchema.setJarProperties(jarHeader.toString());
			isUpdated = true;
		}

		if (isUpdated) {
			m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
			return toSchemaView(metaSchema);
		} else {
			return schemaView;
		}
	}

	/**
	 * 
	 * @param schemaView
	 * @param schemaContent
	 * @param schemaHeader
	 * @param jarContent
	 * @param jarHeader
	 * @throws IOException
	 * @throws DalException
	 */
	public SchemaView uploadJsonSchema(SchemaView schemaView, byte[] schemaContent,
	      FormDataContentDisposition schemaHeader, byte[] jarContent, FormDataContentDisposition jarHeader)
	      throws IOException, DalException {
		if (schemaContent == null) {
			return schemaView;
		}

		boolean isUpdated = false;
		Schema metaSchema = toSchema(schemaView);
		if (schemaContent != null) {
			metaSchema.setSchemaContent(schemaContent);
			metaSchema.setSchemaProperties(schemaHeader.toString());
			isUpdated = true;
		}

		if (jarContent != null) {
			metaSchema.setJarContent(jarContent);
			metaSchema.setJarProperties(jarHeader.toString());
			isUpdated = true;
		}

		if (isUpdated) {
			m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
			return toSchemaView(metaSchema);
		} else {
			return schemaView;
		}
	}

	/**
	 * 
	 * @param name
	 * @param schemaContent
	 * @return
	 * @throws IOException
	 * @throws RestClientException
	 */
	public boolean verifyCompatible(Schema schema, byte[] schemaContent) throws IOException, RestClientException {
		if (schemaContent == null) {
			return false;
		}
		Parser parser = new Parser();
		org.apache.avro.Schema avroSchema = parser.parse(new String(schemaContent));
		boolean result = getAvroSchemaRegistry().testCompatibility(schema.getName(), avroSchema);
		return result;
	}

	/**
	 * 
	 * @param schemaView
	 * @return
	 */
	public static Schema toSchema(SchemaView schemaView) {
		Schema schema = new Schema();
		if (schemaView.getId() != null) {
			schema.setId(schemaView.getId());
		}
		schema.setName(schemaView.getName());
		schema.setType(schemaView.getType());
		if (schemaView.getVersion() != null) {
			schema.setVersion(schemaView.getVersion());
		}
		schema.setCreateTime(schemaView.getCreateTime());
		schema.setCompatibility(schemaView.getCompatibility());
		schema.setDescription(schemaView.getDescription());
		if (schemaView.getTopicId() != null) {
			schema.setTopicId(schemaView.getTopicId());
		}
		return schema;
	}

	/**
	 * 
	 * @param schema
	 * @return
	 */
	public static SchemaView toSchemaView(Schema schema) {
		SchemaView view = new SchemaView();
		view.setId(schema.getId());
		view.setName(schema.getName());
		view.setType(schema.getType());
		view.setVersion(schema.getVersion());
		view.setCreateTime(schema.getCreateTime());
		view.setDescription(schema.getDescription());
		view.setCompatibility(schema.getCompatibility());
		view.setTopicId(schema.getTopicId());
		view.setAvroId(schema.getAvroid());
		if (schema.getSchemaContent() != null) {
			view.setSchemaPreview(new String(schema.getSchemaContent()));
		}
		return view;
	}
}
