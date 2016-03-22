package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.cmessaging.entity.Cmessaging;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.ZookeeperService;
import com.ctrip.hermes.metaservice.zk.ZKPathUtils;
import com.ctrip.hermes.metaservice.zk.ZKSerializeUtils;

@Path("/cmessage/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class CmessageResource {
	private ZookeeperService m_zkService = PlexusComponentLocator.lookup(ZookeeperService.class);

	@POST
	@Path("exchange/update")
	public Response validateTopicView(@FormParam("password") String password, @FormParam("content") String content) {
		if (!"123hermes".equals(password)) {
			return Response.status(Status.BAD_REQUEST).entity("Wrong password").build();
		}
		List<String> list = new ArrayList<String>();
		for (String item : content.split("\n")) {
			item = item.trim();
			if (item.length() > 0) {
				list.add(item);
			}
		}
		try {
			m_zkService.persist(ZKPathUtils.getCmessageExchangePath(), ZKSerializeUtils.serialize(list));
		} catch (Exception e) {
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
		return Response.status(Status.OK).entity(list).build();
	}

	@GET
	@Path("exchange")
	public Response getExchangeInfo() {
		try {
			return Response.status(Status.OK).entity(m_zkService.queryData(ZKPathUtils.getCmessageExchangePath())).build();
		} catch (Exception e) {
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
	}

	@POST
	@Path("config/update")
	public Response updateConfig(@FormParam("password") String password, @FormParam("content") String content) {
		if (!"123hermes".equals(password)) {
			return Response.status(Status.BAD_REQUEST).entity("Wrong password").build();
		}

		if (!StringUtils.isBlank(content)) {
			try {
				JSON.parseObject(content, Cmessaging.class);
				m_zkService.persist(ZKPathUtils.getCmessageConfigPath(), ZKSerializeUtils.serialize(content));
			} catch (Exception e) {
				return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
			}
		}
		return Response.status(Status.OK).entity(content).build();
	}

	@GET
	@Path("config")
	public Response getConfigInfo() {
		try {
			return Response.status(Status.OK).entity(m_zkService.queryData(ZKPathUtils.getCmessageConfigPath())).build();
		} catch (Exception e) {
			return Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}
	}
}
