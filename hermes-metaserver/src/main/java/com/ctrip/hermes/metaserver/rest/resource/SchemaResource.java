package com.ctrip.hermes.metaserver.rest.resource;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.bo.SchemaView;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaserver.rest.commons.RestException;
import com.ctrip.hermes.metaservice.model.Schema;
import com.ctrip.hermes.metaservice.service.SchemaService;

@Path("/schemas/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class SchemaResource {

	private SchemaService schemaService = PlexusComponentLocator.lookup(SchemaService.class);

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
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return returnResult;
	}
}
