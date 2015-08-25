package com.ctrip.hermes.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.AdminServlet;
import com.codahale.metrics.servlets.MetricsServlet;

public class HermesServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5115168561274109694L;

	public static final String DEFAULT_JVM_URI = "/jvm";

	public static final String DEFAULT_HERMES_URI = "/hermes";

	private transient String jvmUri;

	private transient String hermesUri;

	private transient String hermesUriByT;

	private transient String hermesUriByTP;

	private transient String hermesUriByTPG;

	private transient JVMMetricsServlet jvmServlet;

	private transient Map<String, MetricsServlet> metricServletGroupByT;

	private transient Map<String, MetricsServlet> metricServletGroupByTP;

	private transient Map<String, MetricsServlet> metricServletGroupByTPG;

	private static final String CONTENT_TYPE = "text/html";

	private static final String TEMPLATE = String
	      .format("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\"%n"
	            + "        \"http://www.w3.org/TR/html4/loose.dtd\">%n" + "<html>%n" + "<head>%n"
	            + "  <title>Hermes Metrics</title>%n" + "</head>%n" + "<body>%n" + "  <ul>%n"
	            + "    <li><a href=\"{0}{1}?pretty=true\">Metrics By T</a></li>%n"
	            + "    <li><a href=\"{2}{3}?pretty=true\">Metrics By TP</a></li>%n"
	            + "    <li><a href=\"{4}{5}?pretty=true\">Metrics By TPG</a></li>%n"
	            + "    <li><a href=\"{6}{7}?pretty=true\">JVM</a></li>%n" + "  </ul>%n" + "</body>%n" + "</html>");

	public void init(ServletConfig config) throws ServletException {
		if (Boolean.valueOf(config.getInitParameter("show-jvm-metrics"))) {
			this.jvmServlet = new JVMMetricsServlet();
			this.jvmServlet.init(config);

			this.jvmUri = DEFAULT_JVM_URI;
		}

		this.metricServletGroupByT = new HashMap<String, MetricsServlet>();
		this.metricServletGroupByTP = new HashMap<String, MetricsServlet>();
		this.metricServletGroupByTPG = new HashMap<String, MetricsServlet>();
		this.hermesUri = DEFAULT_HERMES_URI;
		this.hermesUriByT = this.hermesUri + "/t/";
		this.hermesUriByTP = this.hermesUri + "/tp/";
		this.hermesUriByTPG = this.hermesUri + "/tpg/";
	}

	@Override
	protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		final String uri = req.getPathInfo();
		if (uri == null) {
			super.service(req, resp);
			return;
		}
		if (jvmUri != null && uri.equals(jvmUri)) {
			jvmServlet.service(req, resp);
		} else if (uri.startsWith(hermesUri)) {
			MetricsServlet metricsServlet = getServletFromUri(uri);
			metricsServlet.service(req, resp);
		} else {
			super.service(req, resp);
		}
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		final String path = req.getContextPath() + req.getServletPath();

		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
		resp.setContentType(CONTENT_TYPE);
		final PrintWriter writer = resp.getWriter();
		try {
			writer.println(MessageFormat.format(TEMPLATE, path, "/t", path, "/tp", path, "/tpg", path, jvmUri));
		} finally {
			writer.close();
		}
	}

	private MetricsServlet getServletFromUri(String fullUri) throws ServletException {
		MetricsServlet metricServlet = null;
		if (fullUri.startsWith(hermesUriByT)) {
			String T = fullUri.substring(hermesUriByT.length());
			if (!metricServletGroupByT.containsKey(T)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByT(T);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(this.getServletConfig());
				metricServletGroupByT.put(T, metricServlet);
			} else {
				metricServlet = metricServletGroupByT.get(T);
			}
		} else if (fullUri.startsWith(hermesUriByTP)) {
			String TP = fullUri.substring(hermesUriByTP.length());
			if (!metricServletGroupByTP.containsKey(TP)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByT(TP);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(this.getServletConfig());
				metricServletGroupByTP.put(TP, metricServlet);
			} else {
				metricServlet = metricServletGroupByTP.get(TP);
			}
		} else if (fullUri.startsWith(hermesUriByTPG)) {
			String TPG = fullUri.substring(hermesUriByTPG.length());
			if (!metricServletGroupByTPG.containsKey(TPG)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByT(TPG);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(this.getServletConfig());
				metricServletGroupByTPG.put(TPG, metricServlet);
			} else {
				metricServlet = metricServletGroupByTPG.get(TPG);
			}
		}
		return metricServlet;
	}
}
