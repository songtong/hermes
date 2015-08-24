package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.unidal.dal.jdbc.DalException;
import org.unidal.dal.jdbc.DalNotFoundException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.schemaregistry.SchemaKey;
import com.ctrip.hermes.metaservice.schemaregistry.SchemaRegistryKeyType;
import com.ctrip.hermes.metaservice.schemaregistry.SchemaValue;
import com.ctrip.hermes.metaservice.service.SchemaRegistryService;

@Path("/schemaregistry/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryResource {

	private SchemaRegistryService schemaRegistryService = PlexusComponentLocator.lookup(SchemaRegistryService.class);

	@Path("subjects/{subject}")
	@POST
	public Response updateSchemaInKafkaStorage(@PathParam("subject") String subject,
	      @QueryParam("version") Integer version, String schema,
	      @QueryParam("schemaTopic") @DefaultValue("_schemas") String topic) {
		SchemaKey schemaKey = new SchemaKey();
		schemaKey.setSubject(subject);
		schemaKey.setVersion(version);
		schemaKey.setMagic(0);
		schemaKey.setKeytype(SchemaRegistryKeyType.SCHEMA);

		SchemaValue schemaValue = null;
		try {
			schemaValue = JSON.parseObject(schema, SchemaValue.class);
		} catch (Exception e) {
			throw new BadRequestException(e);
		}

		boolean result;
		try {
			result = schemaRegistryService.updateSchemaInKafkaStorage(topic, schemaKey, schemaValue);
		} catch (Exception e) {
			throw new InternalServerErrorException(e);
		}

		if (result) {
			return Response.status(Status.OK).build();
		} else {
			return Response.status(Status.INTERNAL_SERVER_ERROR).build();
		}
	}

	@Path("schemas/{schemaId}")
	@GET
	public SchemaValue fetchSchemaFromMetaServer(@PathParam("schemaId") Integer schemaId) {
		SchemaValue schemaValue = null;
		try {
			schemaValue = schemaRegistryService.fetchSchemaFromMetaServer(schemaId);
		} catch (DalNotFoundException e) {
			throw new NotFoundException(e);
		} catch (DalException e) {
			throw new InternalServerErrorException(e);
		}
		if (schemaValue == null) {
			throw new NotFoundException();
		}
		return schemaValue;
	}
}
