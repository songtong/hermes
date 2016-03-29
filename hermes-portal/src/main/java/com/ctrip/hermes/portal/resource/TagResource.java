package com.ctrip.hermes.portal.resource;

import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
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
import org.unidal.dal.jdbc.DalException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.application.ConsumerApplication;
import com.ctrip.hermes.portal.dal.datasourcetag.DatasourceTag;
import com.ctrip.hermes.portal.dal.tag.Tag;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.service.tag.DefaultTagService;
import com.ctrip.hermes.protal.util.ResponseUtils;

@Path("/tags")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TagResource {
	private static final Logger log = LoggerFactory.getLogger(TagResource.class);
	
	private DefaultTagService tagService = PlexusComponentLocator.lookup(DefaultTagService.class);
	
	@GET
	@Path("datasources")
	public Response getDatasourcesTags() {
		Map<String, List<Tag>> datasourcesTags = tagService.listDatasourcesTags();
		
		if (datasourcesTags == null) {
			throw new RestException("Failed to load datasources tags!", Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(ResponseUtils.wrapResponse(Status.OK, datasourcesTags)).build();
	}
	
	@GET
	@Path("datasources/{id}")
	public Response getDatasourceTags(@PathParam("id") String datasourceId) {
		try {
			List<Tag> datasourceTags = tagService.listDatasourceTags(datasourceId);
			
			if (datasourceTags == null) {
				throw new RestException("Failed to load datasources tags!", Status.INTERNAL_SERVER_ERROR);
			}
			
			return Response.status(Status.OK).entity(ResponseUtils.wrapResponse(Status.OK, datasourceTags)).build();
		} catch (DalException e) {
			throw new RestException(e.getMessage(), Status.INTERNAL_SERVER_ERROR);
		}
	}
	
	@POST
	@Path("datasources/{id}")
	public Response addDatasourceTag(@PathParam("id") String datasourceId, @QueryParam("tagId") long tagId, String content) {
		if (tagId == 0 && content == null) {
			throw new RestException(String.format("Either paramater need to be offered: tagId, tagContent!"), Status.BAD_REQUEST);
		}
		
		try {
			DatasourceTag datasourceTag = null;
			
			if (content != null) {
				Tag tag = null;
				try {
					tag = JSON.parseObject(content, Tag.class);
				} catch (Exception e) {
					throw new RestException(String.format("Invalid content for tag: %s", content), Status.BAD_REQUEST);
				}
				
				datasourceTag = tagService.addDatasourceTag(datasourceId, tag);
			} else {
				datasourceTag = tagService.addDatasourceTag(datasourceId, tagId);
			}
			return Response.status(Status.OK).entity(ResponseUtils.wrapResponse(Status.OK, datasourceTag)).build();
		} catch (DalException e) {
			throw new RestException(e.getMessage(), Status.INTERNAL_SERVER_ERROR); 
		}
	}
	
	@DELETE
	@Path("datasources/{id}")
	public Response removeDatasourceTag(@PathParam("id") String datasourceId, @QueryParam("tagId") int tagId) {
		try {
			if (tagService.validateDatasource(datasourceId) && tagService.validateTag(tagId)) {
				DatasourceTag datasourceTag = tagService.removeDatasourceTag(datasourceId, tagId);
				if (datasourceTag == null) {
					throw new RestException("Failed to remove datasource tag mapping!", Status.INTERNAL_SERVER_ERROR);
				}
				return Response.status(Status.OK).entity(ResponseUtils.wrapResponse(Status.OK, datasourceTag)).build();
			} else {
				throw new RestException(String.format("Ensure the existence of datasource & tag: %s, %d!", datasourceId, tagId), Status.BAD_REQUEST);
			}
		} catch (DalException e) {
			throw new RestException(String.format("Failed to verify existence of datasource/tag: %s/%d", datasourceId, tagId), Status.INTERNAL_SERVER_ERROR); 
		}
	}
	
	@GET
	public Response listTags() {
		Map<String, List<Tag>> tags = tagService.listTags();
		
		if (tags == null) {
			throw new RestException("Failed to load tags!", Status.INTERNAL_SERVER_ERROR);
		}
		return Response.status(Status.OK).entity(ResponseUtils.wrapResponse(Status.OK, tags)).build();
	}
}
