package com.ctrip.hermes.portal.web;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

public class SsoApiFilter implements Filter {

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException,
	      ServletException {
		HttpServletRequest req = ((HttpServletRequest) request);
		String path = req.getRequestURI();

		String apiPrefix = "/api/";
		if (path.startsWith(apiPrefix) && !"delete".equalsIgnoreCase(req.getMethod())) {
			request.getRequestDispatcher("/apisso/" + path.substring(apiPrefix.length())).forward(request, response);
		} else {
			chain.doFilter(request, response);
		}
	}

	@Override
	public void destroy() {

	}

}
