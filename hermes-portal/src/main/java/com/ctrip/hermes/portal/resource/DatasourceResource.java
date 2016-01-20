package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.service.StorageService;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/datasources/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class DatasourceResource {

	private static final Logger logger = LoggerFactory.getLogger(DatasourceResource.class);

	private StorageService dsService = PlexusComponentLocator.lookup(StorageService.class);

	@POST
	@Path("{type}")
	public Response addDatasource(@PathParam("type") String dsType, String content) {

		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Datasource datasource;
		try {
			datasource = JSON.parseObject(content, Datasource.class);
		} catch (Exception e) {
			logger.error("Parse consumer failed, content: {}", content, e);
			throw new RestException(e, Status.BAD_REQUEST);
		}

		if (StringUtils.isEmpty(datasource.getId())) {
			throw new RestException("Datasource Id is empty", Status.BAD_REQUEST);
		}

		if (dsService.getDatasources().containsKey(datasource.getId())) {
			throw new RestException(String.format("Datasource id: %s, type: %s, already exists.", datasource.getId(),
			      dsType), Status.CONFLICT);
		}

		try {
			dsService.addDatasource(datasource, dsType);
			return Response.status(Status.CREATED).build();
		} catch (Exception e) {
			logger.error("Add Datasource failed.", e);
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
	}

	@DELETE
	@Path("{type}/{id}")
	public Response deleteDatasource(@PathParam("id") String id, @PathParam("type") String dsType) {
		try {
			dsService.deleteDatasource(id, dsType);
		} catch (Exception e) {
			logger.warn("Delete Datasource failed", e);
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).build();
	}

	@DELETE
	@Path("{type}/{id}/delprop")
	public Response delDsProp(@PathParam("type") String type, @PathParam("id") String id, @QueryParam("name") String name) {
		logger.info("Delete datasource property: {} {}", id, name);
		if (StringUtils.isBlank(id) || StringUtils.isBlank(name)) {
			throw new RestException(String.format("ID: %s or Name: %s is blank", id, name), Status.BAD_REQUEST);
		}

		try {
			Storage storage = dsService.getStorages().get(type);
			List<Datasource> dss = storage.getDatasources();
			for (Datasource ds : dss) {
				if (ds.getId().equals(id) && ds.getProperties().containsKey(name)) {
					ds.getProperties().remove(name);

					dsService.updateDatasource(ds);
					return Response.status(Status.OK).build();
				}
			}
		} catch (Exception e) {
			throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
		}
		throw new RestException(String.format("Property %s not found in %s", name, id), Status.NOT_FOUND);
	}

	@POST
	@Path("{type}/{id}/update")
	public Response updateDatasource(@PathParam("type") String type, String content) {
		if (StringUtils.isEmpty(content)) {
			throw new RestException("HTTP POST body is empty", Status.BAD_REQUEST);
		}

		Storage storage = dsService.getStorages().get(type);
		if (storage == null) {
			throw new RestException("Invalid storage type", Status.NOT_FOUND);
		}

		List<Datasource> datasources = storage.getDatasources();
		Datasource dsn = JSON.parseObject(content, Datasource.class);
		normalizeDatasource(dsn);
		for (int idx = 0; idx < datasources.size(); idx++) {
			Datasource ds = datasources.get(idx);
			if (ds.getId().equals(dsn.getId())) {
				try {
					dsService.updateDatasource(dsn);
				} catch (Exception e) {
					throw new RestException(e, Status.INTERNAL_SERVER_ERROR);
				}
				return Response.status(Status.OK).entity(storage).build();
			}
		}

		throw new RestException("Datasource id not found: " + dsn.getId(), Status.NOT_FOUND);
	}

	private void normalizeDatasource(Datasource ds) {
		List<Property> properties = new ArrayList<Property>(ds.getProperties().values());
		ds.getProperties().clear();
		for (Property p : properties) {
			if (!StringUtils.isBlank(p.getName())) {
				ds.getProperties().put(p.getName(), p);
			}
		}
	}
}
