package com.ctrip.hermes.portal.web;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.jasig.cas.client.util.AssertionHolder;
import org.jasig.cas.client.validation.Assertion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.resource.assists.ValidationUtils;
import com.dianping.cat.Cat;

public class HermesValidationFilter implements Filter {
	private final static Logger log = LoggerFactory.getLogger(HermesValidationFilter.class);

	private PortalConfig m_config = PlexusComponentLocator.lookup(PortalConfig.class);

	private String[] m_protectedPages = { "/console/consumer", "/console/subscription", "/console/storage", "/console/endpoint",
	      "/console/resender" };

	private AtomicReference<Set<String>> m_admins = new AtomicReference<>();

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		InputStream in = this.getClass().getResourceAsStream("/admin.properties");
		Properties p = new Properties();
		try {
			p.load(in);
		} catch (IOException e) {
			throw new ServletException("Can not load admin.properties", e);
		}

		Set<String> adminSet = new HashSet<>();
		String adminString = p.getProperty("admins");
		String[] admins = adminString.split(",");
		for (String admin : admins) {
			if (StringUtils.isNotBlank(admin)) {
				adminSet.add(admin.trim());
			}
		}
		m_admins.set(adminSet);
		log.info("admins: {}", m_admins);
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		HttpServletRequest req = (HttpServletRequest) request;
		HttpServletResponse res = (HttpServletResponse) response;

		Assertion assertion = AssertionHolder.getAssertion();
		String user = null;
		if (assertion != null && assertion.getPrincipal() != null && assertion.getPrincipal().getAttributes() != null) {
			user = (String) assertion.getPrincipal().getAttributes().get("name");
		}

		// log user name to cat
		if (user != null) {
			Cat.logEvent("Hermes.Portal.User", user);
		}

		// reject unauthorized Delete operation
		if (req.getRequestURI().startsWith("/api") && "delete".equalsIgnoreCase(req.getMethod())) {
			if (!isAdmin(user)) {
				log.warn("User:{} from ip:{} attemp to call unauthorized url:{}", user, req.getRemoteAddr(), req.getRequestURL()
				      .toString());
				res.sendRedirect("/console");
				return;
			}
		}

		boolean isLogined = false;
		if (req.getCookies() != null) {
			try {
				String token = getToken(req);
				if (StringUtils.isNotEmpty(token)) {
					isLogined = validateCookie(token);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		String requestUrl = ((HttpServletRequest) request).getRequestURI();
		String apiPrefix = "/api/";
		if (isLogined == false) {
			for (String page : m_protectedPages) {
				if (requestUrl.startsWith(page)) {
					res.sendRedirect("/console");
					return;
				}
			}
		}
		request.setAttribute("logined", isLogined);
		if (isLogined && requestUrl.startsWith(apiPrefix)) {
			request.getRequestDispatcher("/apisso/" + requestUrl.substring(apiPrefix.length())).forward(request, response);
		} else {
			chain.doFilter(request, response);
		}
	}

	private boolean isAdmin(String ssoUser) {
		return ssoUser != null && m_admins.get().contains(ssoUser);
	}

	private String getToken(HttpServletRequest request) {
		String header = request.getHeader("Cookie");
		if (header == null) {
			return "";
		}

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

	private boolean validateCookie(String cookie) throws Exception {
		String username = m_config.getAccount().getKey();
		String pwd = m_config.getAccount().getValue();
		String cookieDecoded = ValidationUtils.decode(cookie);
		return (username + pwd).equals(cookieDecoded);
	}

	@Override
	public void destroy() {
	}

}
