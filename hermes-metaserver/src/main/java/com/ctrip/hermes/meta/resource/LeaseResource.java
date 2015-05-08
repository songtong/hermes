package com.ctrip.hermes.meta.resource;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.bo.Tpg;
import com.ctrip.hermes.core.lease.Lease;

@Path("/lease/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class LeaseResource {

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("acquire")
	public Lease tryAcquireLease(Tpg tpg) {
		// TODO
		return new Lease(System.currentTimeMillis() + 10 * 1000);
	}

}
