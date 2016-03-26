package com.ctrip.hermes.portal.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.session.ExpiredSessionException;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.Factory;
import org.apache.shiro.util.ThreadContext;
import org.jasig.cas.client.util.AssertionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.cat.Cat;

public class HermesValidationFilter implements Filter {
	private final static Logger LOGGER = LoggerFactory.getLogger(HermesValidationFilter.class);
	private final static String API_PREFIX = "/api/";

	private String[] m_protectedPages = { "/console/consumer", "/console/subscription", "/console/storage", "/console/endpoint",
	      "/console/resender" };

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		Factory<org.apache.shiro.mgt.SecurityManager> factory = new IniSecurityManagerFactory("classpath:shiro.ini");
		SecurityUtils.setSecurityManager(factory.getInstance());
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		HttpServletRequest req = (HttpServletRequest)request;
		HttpServletResponse res = (HttpServletResponse)response;
		boolean isAdmin = false;
		String user = null;
		
		if (AssertionHolder.getAssertion() != null) {
			Subject subject = SecurityUtils.getSubject();
			try {
				user = (String) subject.getPrincipal();
			} catch (ExpiredSessionException | UnknownSessionException e ) {
				ThreadContext.unbindSubject();
				subject = SecurityUtils.getSubject();
			}
			isAdmin = subject.hasRole("admin");
			Cat.logEvent("Hermes.Portal.User", user);
		}
		
		String requestUrl = ((HttpServletRequest) request).getRequestURI();

		// Reject unauthorized Delete operation
		if (requestUrl.startsWith(API_PREFIX) && "delete".equalsIgnoreCase(req.getMethod())) {
			if (!isAdmin) {
				LOGGER.warn("User:{} from ip:{} attemp to call unauthorized url:{}", user, req.getRemoteAddr(), req.getRequestURL()
				      .toString());
				res.sendRedirect("/console");
				return;
			}
		}
		
		if (!isAdmin) {
			for (String page : m_protectedPages) {
				if (requestUrl.startsWith(page)) {
					res.sendRedirect("/console");
					return;
				}
			}
		}
		
		if (requestUrl.startsWith(API_PREFIX)) {
			request.getRequestDispatcher("/apisso/" + requestUrl.substring(API_PREFIX.length())).forward(request, response);
		} else {
			chain.doFilter(request, response);
		}
	}

	@Override
	public void destroy() {
	}
}
