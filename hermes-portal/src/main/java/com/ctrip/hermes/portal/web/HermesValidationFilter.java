package com.ctrip.hermes.portal.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.resource.assists.ValidationUtils;

public class HermesValidationFilter implements Filter {
	private PortalConfig m_config = PlexusComponentLocator.lookup(PortalConfig.class);

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {

	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
			throws IOException, ServletException {
		HttpServletRequest req = (HttpServletRequest) request;
		boolean isLogined = false;
		if (req.getCookies() != null) {
			try {
				isLogined = validateCookie(getToken(req), request.getRemoteAddr());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		request.setAttribute("logined", isLogined);
		chain.doFilter(request, response);
	}

	private String getToken(HttpServletRequest request) {
		String header = request.getHeader("Cookie");
		for (String part : header.split("; ")) {
			int sep = part.indexOf("=");
			if (sep > 0) {
				String cookieName = part.substring(0, sep);
				if (cookieName.equals("_token")) {
					return part.substring(sep + 1);
				}
			}
		}
		return "";
	}

	private boolean validateCookie(String cookie, String ip) throws Exception {
		String username = m_config.getAccount().getKey();
		String pwd = m_config.getAccount().getValue();
		String cookieDecoded = ValidationUtils.decode(cookie);
		return (username + pwd + ip).equals(cookieDecoded);
	}

	@Override
	public void destroy() {
	}

}
