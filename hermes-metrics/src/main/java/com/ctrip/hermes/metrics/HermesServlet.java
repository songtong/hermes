package com.ctrip.hermes.metrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;

public class HermesServlet extends HttpServlet {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5115168561274109694L;

	private String jvmUri;

	private String healthcheckUri;

	private String pingUri;

	private String metricsUri;

	private String threadsUri;

	private String globalUri;

	private JVMMetricsServlet jvmServlet;

	private HealthCheckServlet healthCheckServlet;

	private PingServlet pingServlet;

	private ThreadDumpServlet threadDumpServlet;

	private MetricsServlet metricServlet;

	private Map<String, MetricsServlet> metricServletGroupByT;

	private Map<String, MetricsServlet> metricServletGroupByTP;

	private Map<String, MetricsServlet> metricServletGroupByTPG;

	private static final String CONTENT_TYPE = "text/html";

	private ServletConfig config;

	public void init(ServletConfig config) throws ServletException {
		this.config = config;
		if (Boolean.valueOf(config.getInitParameter("show-jvm-metrics"))) {
			JVMMetricsServletContextListener listener = new JVMMetricsServletContextListener();
			this.jvmServlet = new JVMMetricsServlet(listener.getMetricRegistry());
			this.jvmServlet.init(config);

			this.jvmUri = "/jvm";
		}

		healthCheckServlet = new HealthCheckServlet(HermesMetricsRegistry.getHealthCheckRegistry());
		healthCheckServlet.init(config);

		pingServlet = new PingServlet();
		pingServlet.init(config);

		threadDumpServlet = new ThreadDumpServlet();
		threadDumpServlet.init(config);

		metricServlet = new MetricsServlet(HermesMetricsRegistry.getMetricRegistry());
		metricServlet.init(config);

		this.metricServletGroupByT = new HashMap<String, MetricsServlet>();
		this.metricServletGroupByTP = new HashMap<String, MetricsServlet>();
		this.metricServletGroupByTPG = new HashMap<String, MetricsServlet>();

		this.healthcheckUri = "/healthcheck";
		this.pingUri = "/ping";
		this.threadsUri = "/threads";
		this.globalUri = "/global";
		this.metricsUri = "/metrics";
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
		} else if (uri.equals(healthcheckUri)) {
			healthCheckServlet.service(req, resp);
		} else if (uri.equals(globalUri)) {
			metricServlet.service(req, resp);
		} else if (uri.equals(pingUri)) {
			pingServlet.service(req, resp);
		} else if (uri.equals(threadsUri)) {
			threadDumpServlet.service(req, resp);
		} else if (uri.startsWith("/t/")) {
			MetricsServlet metricsServlet = getServletFromUri(uri);
			metricsServlet.service(req, resp);
		} else if (uri.startsWith("/tp/")) {
			MetricsServlet metricsServlet = getServletFromUri(uri);
			metricsServlet.service(req, resp);
		} else if (uri.startsWith("/tpg/")) {
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
			StringBuilder sb = new StringBuilder();
			sb.append("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\"")
			      .append("\"http://www.w3.org/TR/html4/loose.dtd\"><html><head>")
			      .append("<title>Hermes Metrics</title></head><body>");
			sb.append("<h3>Default Metrics</h3>");
			sb.append("<ul>");
			sb.append("<li><a href=\"").append(path).append(globalUri).append("?pretty=true\">Global</a></li>");
			sb.append("<li><a href=\"").append(path).append(threadsUri).append("?pretty=true\">Threads Dump</a></li>");
			sb.append("<li><a href=\"").append(path).append(jvmUri).append("?pretty=true\">JVM</a></li>");
			sb.append("<li><a href=\"").append(path).append(pingUri).append("?pretty=true\">Ping</a></li>");
			sb.append("<li><a href=\"").append(path).append(healthcheckUri).append("?pretty=true\">Health Check</a></li>");
			sb.append("</ul>");
			sb.append("<h3>Metrics By T</h3>");
			sb.append("<ul>");
			Set<String> tKeys = HermesMetricsRegistry.getMetricRegistiesGroupByT().keySet();
			for (String t : tKeys) {
				sb.append("<li><a href=\"").append(metricsUri).append("/t/").append(t).append("?pretty=true\">").append(t)
				      .append("</a></li>");
			}
			sb.append("</ul>");
			sb.append("<h3>Metrics By TP</h3>");
			sb.append("<ul>");
			Set<String> tpKeys = HermesMetricsRegistry.getMetricRegistiesGroupByTP().keySet();
			for (String tp : tpKeys) {
				sb.append("<li><a href=\"").append(metricsUri).append("/tp/").append(tp).append("?pretty=true\">")
				      .append(tp).append("</a></li>");
			}
			sb.append("</ul>");
			sb.append("<h3>Metrics By TPG</h3>");
			sb.append("<ul>");
			Set<String> tpgKeys = HermesMetricsRegistry.getMetricRegistiesGroupByTPG().keySet();
			for (String tpg : tpgKeys) {
				sb.append("<li><a href=\"").append(metricsUri).append("/tpg/").append(tpg).append("?pretty=true\">")
				      .append(tpg).append("</a></li>");
			}
			sb.append("</ul>");
			sb.append("</body></html>");
			writer.println(sb.toString());
		} finally {
			writer.close();
		}
	}

	private MetricsServlet getServletFromUri(String fullUri) throws ServletException {
		MetricsServlet metricServlet = null;
		if (fullUri.startsWith("/t/")) {
			String T = fullUri.substring("/t/".length());
			if (!metricServletGroupByT.containsKey(T)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByT(T);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(config);
				metricServletGroupByT.put(T, metricServlet);
			} else {
				metricServlet = metricServletGroupByT.get(T);
			}
		} else if (fullUri.startsWith("/tp/")) {
			String TP = fullUri.substring("/tp/".length());
			if (!metricServletGroupByTP.containsKey(TP)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByT(TP);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(config);
				metricServletGroupByTP.put(TP, metricServlet);
			} else {
				metricServlet = metricServletGroupByTP.get(TP);
			}
		} else if (fullUri.startsWith("/tpg/")) {
			String TPG = fullUri.substring("/tpg/".length());
			if (!metricServletGroupByTPG.containsKey(TPG)) {
				MetricRegistry metricRegistry = HermesMetricsRegistry.getMetricRegistryByT(TPG);
				metricServlet = new MetricsServlet(metricRegistry);
				metricServlet.init(config);
				metricServletGroupByTPG.put(TPG, metricServlet);
			} else {
				metricServlet = metricServletGroupByTPG.get(TPG);
			}
		}
		return metricServlet;
	}
}
