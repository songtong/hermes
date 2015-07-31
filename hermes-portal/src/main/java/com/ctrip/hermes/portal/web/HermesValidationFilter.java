package com.ctrip.hermes.portal.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
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
			for (Cookie cookie : req.getCookies()) {
				if ("_token".equals(cookie.getName())) {
					try {
						isLogined = validateCookie(cookie.getValue(), request.getRemoteAddr());
					} catch (Exception e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}
		request.setAttribute("logined", isLogined);
		chain.doFilter(request, response);
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
