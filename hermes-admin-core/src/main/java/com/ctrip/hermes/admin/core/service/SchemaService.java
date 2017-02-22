package com.ctrip.hermes.admin.core.service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import org.apache.avro.Protocol;
import org.apache.avro.Schema.Parser;
import org.apache.avro.compiler.idl.ParseException;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.transaction.TransactionManager;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.lookup.util.StringUtils;

import com.ctrip.hermes.admin.core.dal.CachedSchemaDao;
import com.ctrip.hermes.admin.core.dal.CachedTopicDao;
import com.ctrip.hermes.admin.core.model.Schema;
import com.ctrip.hermes.admin.core.model.SchemaEntity;
import com.ctrip.hermes.admin.core.model.TopicEntity;
import com.ctrip.hermes.admin.core.view.SchemaView;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Topic;

@Named
public class SchemaService {

	private static final Logger m_logger = LoggerFactory.getLogger(SchemaService.class);

	@Inject
	private TransactionManager tm;

	private SchemaRegistryClient avroSchemaRegistry;

	@Inject
	private CachedTopicDao m_topicDao;

	@Inject
	private CachedSchemaDao m_schemaDao;

	@Inject
	private CompileService m_compileService;

	@Inject
	private CodecService m_codecService;

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
		List<Schema> schemas = listSchemaMeta(topic.getId());
		if (schemas == null || schemas.size() == 0) {
			return false;
		}
		for (Schema schema : schemas) {
			if (schema.getAvroid() != null && schema.getAvroid() == avroid) {
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
	public void compileAvroToJar(Schema metaSchema, org.apache.avro.Schema avroSchema) throws IOException, DalException {
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

	public void compileAvroToCs(Schema metaSchema, byte[] schemaContent) throws Exception {
		m_logger.info("Compile {} to .cs file", metaSchema.getName());
		final Path csDir = Files.createTempDirectory("avroschema");
		Path schemaFile = Files.createTempFile(metaSchema.getName(), ".avsc");

		FileWriter fw = new FileWriter(schemaFile.toFile());
		fw.write(new String(schemaContent));
		fw.close();

		m_compileService.compileSchemaToCs(schemaFile.toFile(), csDir);

		File zipFile = new File(Files.createTempDirectory("jarFile") + File.separator + "schema.zip");
		zip(csDir, zipFile);
		byte[] zipContent = Files.readAllBytes(zipFile.toPath());
		metaSchema.setCsContent(zipContent);
		FormDataContentDisposition disposition = FormDataContentDisposition.name(metaSchema.getName())
		      .creationDate(new Date(System.currentTimeMillis()))
		      .fileName(metaSchema.getName() + "_" + metaSchema.getVersion() + ".zip").size(zipFile.length()).build();
		metaSchema.setCsProperties(disposition.toString());
		m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
		Files.delete(zipFile.toPath());
		m_compileService.delete(csDir);

	}

	/**
	 * 
	 * @param schemaView
	 * @param topicId
	 * @return
	 * @throws Exception
	 */
	public SchemaView createSchema(Schema schema, Topic topic) throws Exception {
		m_logger.info("Creating schema for topic: {}, schema: {}", topic.getName(), schema);
		try {
			tm.startTransaction("fxhermesmetadb");
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

			com.ctrip.hermes.admin.core.model.Topic topicModel = m_topicDao.findByName(topic.getName());
			topicModel.setSchemaId(schema.getId());
			m_topicDao.updateByPK(topicModel, TopicEntity.UPDATESET_FULL);

			if ("avro".equals(schema.getType())) {
				if (StringUtils.isNotEmpty(schema.getCompatibility())) {
					getAvroSchemaRegistry().updateCompatibility(schema.getName(), schema.getCompatibility());
				}
			}
			tm.commitTransaction();
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
		}
		m_logger.info("Created schema: {}", schema);
		return toSchemaView(schema);
	}

	/**
	 * 
	 * @param id
	 * @throws Exception
	 */
	public void deleteSchema(long id) throws Exception {
		m_logger.info("Deleting schema id: {}", id);
		try {
			tm.startTransaction("fxhermesmetadb");
			Schema schema = m_schemaDao.findByPK(id, SchemaEntity.READSET_FULL);
			com.ctrip.hermes.admin.core.model.Topic topic = m_topicDao.findByPK(schema.getTopicId());
			if (topic != null) {
				List<Schema> schemas = m_schemaDao.findByTopic(topic.getId(), SchemaEntity.READSET_FULL);
				for (Schema s : schemas) {
					if (s.getId() == id) {
						schemas.remove(s);
						break;
					}
				}

				com.ctrip.hermes.admin.core.model.Topic topicModel = m_topicDao.findByName(topic.getName());
				if (schemas.size() > 0 && topic.getSchemaId() != schemas.get(0).getId()) {
					topicModel.setSchemaId(schemas.get(0).getId());
				} else if (schemas.size() == 0) {
					topicModel.setSchemaId(null);
				}
				m_topicDao.updateByPK(topicModel, TopicEntity.UPDATESET_FULL);

				m_schemaDao.deleteByPK(schema);
				m_logger.info("Deleted schema id: {}", id);
				tm.commitTransaction();
			}
		} catch (Exception e) {
			tm.rollbackTransaction();
			throw e;
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
	 * @throws DalException
	 */
	public void deployToMaven(Schema metaSchema, String groupId, String artifactId, String version, String repositoryId)
	      throws NumberFormatException, IOException, java.text.ParseException, DalException, Exception {
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

		metaSchema.setDependencyString(getDependencyString(groupId, artifactId, version, repositoryId));
		m_schemaDao.updateByPK(metaSchema, SchemaEntity.UPDATESET_FULL);
		m_logger.info("Deployed groupId {}, artifactId {}, version {}, repositoryId {}", groupId, artifactId, version,
		      repositoryId);
	}

	private String getDependencyString(String groupId, String artifactId, String version, String repositoryId) {
		StringBuilder sb = new StringBuilder();
		sb.append("<dependency>\r\n\t<groupId>").append(groupId).append("</groupId>\r\n");
		sb.append("\t<artifactId>").append(artifactId).append("</artifactId>\r\n");
		if ("snapshots".equals(repositoryId)) {
			sb.append("\t<version>").append(version).append("-SNAPSHOT</version>\r\n");
		} else if ("releases".equals(repositoryId)) {
			sb.append("\t<version>").append(version).append("</version>\r\n");
		}
		sb.append("</dependency>");
		return sb.toString();
	}

	private SchemaRegistryClient getAvroSchemaRegistry() throws IOException {
		if (avroSchemaRegistry == null) {
			Codec avroCodec = m_codecService.getCodecs().get("avro");
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
		Schema schema = m_schemaDao.findByPK(schemaId);
		return schema;
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 * @throws DalException
	 */
	public SchemaView getSchemaView(long schemaId) throws DalException {
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
	public List<Schema> listSchemaMeta(Long topicId) throws DalException {
		List<Schema> schemas = m_schemaDao.findByTopic(topicId, SchemaEntity.READSET_FULL);
		return schemas;
	}

	/**
	 * 
	 * @param topic
	 * @return
	 * @throws DalException
	 */
	public List<SchemaView> listSchemaView(Long topicId) throws DalException {
		List<SchemaView> result = new ArrayList<SchemaView>();
		List<Schema> schemaMetas = listSchemaMeta(topicId);
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
	 * @throws Exception
	 */
	public SchemaView updateSchemaFile(SchemaView schemaView, byte[] fileContent, FormDataContentDisposition fileHeader)
	      throws Exception {
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
	 * @throws Exception
	 */
	public SchemaView uploadAvroSchema(SchemaView schemaView, byte[] schemaContent,
	      FormDataContentDisposition schemaHeader, byte[] jarContent, FormDataContentDisposition jarHeader)
	      throws Exception {
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

				compileAvroToJar(metaSchema, avroSchema);
				compileAvroToCs(metaSchema, schemaContent);

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
		schema.setDependencyString(schemaView.getDependencyString());
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
		if (schema.getAvroid() != null) {
			view.setAvroId(schema.getAvroid());
		}
		view.setDependencyString(schema.getDependencyString());
		if (schema.getSchemaContent() != null) {
			view.setSchemaPreview(new String(schema.getSchemaContent()));
		}
		return view;
	}

	private void zip(Path src, File destZipFile) throws Exception {
		m_logger.debug("zip destDir {}, zipFile {}", src.getFileName(), destZipFile);
		try {
			ZipFile zipFile = new ZipFile(destZipFile);

			ZipParameters parameters = new ZipParameters();
			parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
			parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);

			if (src.toFile().isDirectory())
				zipFile.addFolder(src.toFile(), parameters);
			if (src.toFile().isFile())
				zipFile.addFile(src.toFile(), parameters);
		} catch (ZipException e) {
			e.printStackTrace();
		}

	}
}
