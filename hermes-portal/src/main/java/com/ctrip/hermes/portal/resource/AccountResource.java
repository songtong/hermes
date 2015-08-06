package com.ctrip.hermes.portal.resource;

import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.resource.assists.RestException;
import com.ctrip.hermes.portal.resource.assists.ValidationUtils;

@Path("/account/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class AccountResource {
	private PortalConfig m_config = PlexusComponentLocator.lookup(PortalConfig.class);

	@POST
	@Path("login")
	public Response login(@Context HttpServletRequest request, @QueryParam("name") String name,
			@QueryParam("password") String password) throws Exception {

		String username = m_config.getAccount().getKey();
		String pwd = m_config.getAccount().getValue();
		if (username.equals(name) && pwd.equals(password)) {
			String cookieValue = null;
			
			String content = name + password;
			try {
				cookieValue = ValidationUtils.encode(content);
			} catch (Throwable e) {
				e.printStackTrace();
			}

			Cookie cookie = new Cookie("_token", cookieValue, "/", null);
			return Response.status(Status.OK).cookie(new NewCookie(cookie)).build();
		}
		throw new RestException("Username or password is invalid!", Status.BAD_REQUEST);
	}
}
