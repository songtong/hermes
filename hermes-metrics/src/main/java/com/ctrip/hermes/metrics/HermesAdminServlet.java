package com.ctrip.hermes.metrics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.AdminServlet;
import com.codahale.metrics.servlets.MetricsServlet;

public class HermesAdminServlet extends AdminServlet {
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

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
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
