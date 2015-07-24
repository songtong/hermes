package com.ctrip.hermes.rest.metrics;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.servlets.AdminServlet;

public class HermesAdminServlet extends AdminServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5115168561274109694L;

	public static final String DEFAULT_JVM_URI = "/jvm";

	private transient String jvmUri;

	private transient JVMMetricsServlet jvmServlet;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		this.jvmServlet = new JVMMetricsServlet();
		this.jvmServlet.init(config);

		this.jvmUri = DEFAULT_JVM_URI;
	}

	@Override
	protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		final String uri = req.getPathInfo();
		if (uri != null && uri.equals(jvmUri)) {
			jvmServlet.service(req, resp);
		} else {
			super.service(req, resp);
		}
	}
	
}
