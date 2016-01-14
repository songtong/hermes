package com.ctrip.hermes.portal.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.metaservice.service.StorageService;

@Path("/storages/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class StorageResource {

	private StorageService dsService = PlexusComponentLocator.lookup(StorageService.class);

	@GET
	public Response getStorages(@QueryParam("type") String type) {
		List<Storage> storages = StringUtils.isBlank(type) ? new ArrayList<Storage>(dsService.getStorages().values())
		      : Arrays.asList(dsService.getStorages().get(type));
		Collections.sort(storages, new Comparator<Storage>() {
			@Override
			public int compare(Storage o1, Storage o2) {
				return o1.getType().compareTo(o2.getType());
			}
		});
		return Response.status(Status.OK).entity(storages).build();
	}
}
