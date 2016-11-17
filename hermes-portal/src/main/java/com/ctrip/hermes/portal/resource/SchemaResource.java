package com.ctrip.hermes.portal.resource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.admin.core.model.Schema;
import com.ctrip.hermes.admin.core.service.SchemaService;
import com.ctrip.hermes.admin.core.service.TopicService;
import com.ctrip.hermes.admin.core.view.SchemaView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Codec;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.mail.PortalMailService;
import com.google.common.io.ByteStreams;

@Path("/schemas/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource {

	private static final Logger logger = LoggerFactory.getLogger(SchemaResource.class);

	private SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

	private PortalMailService mailService = PlexusComponentLocator.lookup(PortalMailService.class);

	private TopicService topicService = PlexusComponentLocator.lookup(TopicService.class);

	/**
	 * 
	 * @param fileInputStream
	 * @param fileHeader
	 * @param jarInputStream
	 * @param jarHeader
	 * @param content
	 * @param topicId
	 * @return
	 */
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response createSchema(@FormDataParam("file") InputStream fileInputStream,
	      @FormDataParam("file") FormDataContentDisposition fileHeader, @FormDataParam("schema") String content,
	      @FormDataParam("topicId") long topicId, @FormDataParam("userName") String userName,
	      @FormDataParam("userMail") String userMail) {
		logger.debug("create schema, topicId {}, content {}, fileHeader {}", topicId, content, fileHeader);
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}
		SchemaView schemaView = null;
		try {
			schemaView = JSON.parseObject(content, SchemaView.class);
		} catch (Exception e) {
			logger.warn("parse schema failed, content {}", content);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		Topic topic = topicService.findTopicEntityById(topicId);
		if (topic == null) {
			throw new RestException("Topic not found: " + topicId, Status.NOT_FOUND);
		}

		if (topic.getCodecType().isEmpty()) {
			throw new RestException("Topic codec type can not be null", Status.BAD_REQUEST);
		}

		String rawCodecType = topic.getCodecType().split(",")[0];
		if (Codec.JSON.equals(rawCodecType)) {
			throw new RestException("Do not support uploading schema for json codec type topics!", Status.BAD_REQUEST);
		}

		if (Codec.AVRO.equals(rawCodecType) && !fileHeader.getFileName().endsWith(".avsc")) {
			throw new RestException("Schema file name must end with .avsc", Status.BAD_REQUEST);
		}

		byte[] fileContent = null;
		if (fileInputStream != null) {
			try {
				fileContent = ByteStreams.toByteArray(fileInputStream);
			} catch (IOException e) {
				logger.warn("Read file input stream failed", e);
				throw new RestException(e, Status.BAD_REQUEST);
			}
		} else {
			if ("avro".equalsIgnoreCase(schemaView.getType())) {
				throw new RestException("avro schema file needed.", Status.BAD_REQUEST);
			}
		}

		try {
			int avroid = -1;
			// If the avro schema has been created, return CONFLICT
			if ("avro".equalsIgnoreCase(schemaView.getType())) {
				avroid = schemaService.checkAvroSchema(topic.getName() + "-value", fileContent);
				if (schemaService.isAvroSchemaExist(topic, avroid)) {
					schemaView = schemaService.getSchemaView(topic.getSchemaId());
					return Response.status(Status.CONFLICT).entity(schemaView).build();
				}
			}
			schemaView = schemaService.createSchema(SchemaService.toSchema(schemaView), topic);
			schemaView = schemaService.updateSchemaFile(schemaView, fileContent, fileHeader);
		} catch (Exception e) {
			logger.warn("Create schema failed", e);
			if (schemaView.getId() != null) {
				try {
					schemaService.deleteSchema(schemaView.getId());
				} catch (Exception e1) {
					throw new RestException(e1, Status.INTERNAL_SERVER_ERROR);
				}
			}
			throw new RestException(e.getMessage(), Status.INTERNAL_SERVER_ERROR);
		}
		mailService.sendUploadSchemaMail(schemaView, userMail, userName);
		return Response.status(Status.CREATED).entity(schemaView).build();
	}

	@POST
	@Path("{topicName}")
	public Response createSchemaByEntity(@PathParam("topicName") String topicName,
	      @QueryParam("userName") String userName, @QueryParam("userMail") String userMail, String schemaContent) {
		logger.debug("create schema with content", schemaContent);
		if (StringUtils.isEmpty(schemaContent)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Schema schema = null;
		try {
			schema = JSON.parseObject(schemaContent, Schema.class);
		} catch (Exception e) {
			logger.error("Can not parse payload: {}, create schema failed.", schemaContent);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		Topic topic = topicService.findTopicEntityByName(topicName);
		if (topic == null) {
			throw new RestException("Topic not found: " + topicName, Status.NOT_FOUND);
		}

		if (topic.getCodecType().isEmpty()) {
			throw new RestException("Topic codec type is null!", Status.BAD_REQUEST);
		}

		String rawCodec = topic.getCodecType().split(",")[0];
		if (!Codec.AVRO.equals(rawCodec)) {
			throw new RestException("Topic " + topicName + " codec type is not avro!", Status.BAD_REQUEST);
		}

		SchemaView schemaView = null;
		try {
			int avroid = -1;
			// If the avro schema has been created, return CONFLICT
			avroid = schemaService.checkAvroSchema(topic.getName() + "-value", schema.getSchemaContent());
			if (schemaService.isAvroSchemaExist(topic, avroid)) {
				schemaView = schemaService.getSchemaView(topic.getSchemaId());
				return Response.status(Status.CONFLICT).entity(schemaView).build();
			}
			schema.setAvroid(avroid);
			schemaView = schemaService.createSchema(schema, topic);
		} catch (Exception e) {
			logger.warn("Create schema failed", e);
			throw new RestException(e.getMessage(), Status.INTERNAL_SERVER_ERROR);
		}
		if (userName != null && userMail != null) {
			mailService.sendUploadSchemaMail(schemaView, userMail, userName);
		}
		return Response.status(Status.CREATED).entity(schemaView).build();

	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 */
	@DELETE
	@Path("{id}")
	public Response deleteSchema(@PathParam("id") long schemaId) {
		logger.debug("delete schema {}", schemaId);
		try {
			schemaService.deleteSchema(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("Delete schema failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 */
	@GET
	@Path("{id}/schema")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadSchema(@PathParam("id") long schemaId) {
		logger.debug("download schema {}", schemaId);
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("Download schema failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		String fileProperties = schema.getSchemaProperties();
		if (StringUtils.isEmpty(fileProperties)) {
			throw new RestException("Schema file not found: " + schemaId, Status.NOT_FOUND);
		}

		return Response.status(Status.OK).header("content-disposition", fileProperties).entity(schema.getSchemaContent())
		      .build();
	}

	/**
	 * 
	 * @param schemaId
	 * @return
	 */
	@GET
	@Path("{id}/jar")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadJar(@PathParam("id") long schemaId) {
		logger.debug("download jar {}", schemaId);
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("Download jar failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		String fileProperties = schema.getJarProperties();
		if (StringUtils.isEmpty(fileProperties)) {
			throw new RestException("Schema file not found: " + schemaId, Status.NOT_FOUND);
		}

		return Response.status(Status.OK).header("content-disposition", fileProperties).entity(schema.getJarContent())
		      .build();
	}

	@GET
	@Path("{id}/cs")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public Response downloadCs(@PathParam("id") long schemaId) {
		logger.debug("download cs {}", schemaId);
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("Download jar failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		String fileProperties = schema.getCsProperties();
		if (StringUtils.isEmpty(fileProperties)) {
			throw new RestException("Schema file not found: " + schemaId, Status.NOT_FOUND);
		}

		return Response.status(Status.OK).header("content-disposition", fileProperties).entity(schema.getCsContent())
		      .build();
	}

	@POST
	@Path("{id}/deploy")
	public Response deployMaven(@PathParam("id") long schemaId, @QueryParam("groupId") String groupId,
	      @QueryParam("artifactId") String artifactId, @QueryParam("version") String version,
	      @QueryParam("repositoryId") @DefaultValue("snapshots") String repositoryId) {
		logger.debug("deploy maven {} {} {} {} {}", schemaId, groupId, artifactId, version, repositoryId);
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
			schemaService.deployToMaven(schema, groupId, artifactId, version, repositoryId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("Deploy jar failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		return Response.status(Status.OK).build();
	}

	/**
	 * 
	 * @param name
	 * @return
	 */
	@GET
	@Path("{id}")
	public SchemaView getSchema(@PathParam("id") long schemaId) {
		logger.debug("get schema {}", schemaId);
		SchemaView schema = null;
		try {
			schema = schemaService.getSchemaView(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("get schema failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return schema;
	}

	/**
	 * 
	 * @param schemaName
	 * @return
	 */
	@GET
	public List<SchemaView> findSchemas() {
		List<SchemaView> returnResult = new ArrayList<SchemaView>();
		try {
			List<Schema> schemaMetas = schemaService.listLatestSchemaMeta();
			for (Schema schema : schemaMetas) {
				SchemaView schemaView = SchemaService.toSchemaView(schema);
				returnResult.add(schemaView);
			}
		} catch (Exception e) {
			logger.warn("list latest schema failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}

	@POST
	@Path("{id}/compatibility")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response compatibility(@PathParam("id") Long schemaId, @FormDataParam("file") InputStream fileInputStream,
	      @FormDataParam("file") FormDataContentDisposition fileHeader) {
		logger.debug("test compatilibity schemaId {} fileHeader {}", schemaId, fileHeader);
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("get schema failed, schemaId {}", schemaId);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		boolean result = false;
		try {
			byte[] fileContent = ByteStreams.toByteArray(fileInputStream);
			result = schemaService.verifyCompatible(schema, fileContent);
		} catch (Exception e) {
			logger.warn("Read input stream failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		Map<String, Object> entity = new HashMap<>();
		entity.put("is_compatible", result);
		return Response.status(Status.OK).entity(entity).build();
	}

	@GET
	@Path("{id}/compatibility")
	public Response getCompatibility(@PathParam("id") Long schemaId) {
		logger.debug("get compatibility {}", schemaId);
		Schema schema = null;
		try {
			schema = schemaService.getSchemaMeta(schemaId);
		} catch (DalNotFoundException e) {
			throw new RestException("Schema not found: " + schemaId, Status.NOT_FOUND);
		} catch (Exception e) {
			logger.warn("get schema failed, schemaId {}", schemaId);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}

		String result = null;
		try {
			result = schemaService.getCompatible(schema);
		} catch (Exception e) {
			logger.warn("get compatible failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(result).build();
	}
}
