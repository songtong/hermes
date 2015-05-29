package com.ctrip.hermes.rest;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.rest.filter.CORSResponseFilter;
import com.ctrip.hermes.rest.filter.CharsetResponseFilter;
import com.ctrip.hermes.rest.filter.ObjectMapperProvider;
import com.dianping.cat.Cat;

@Named
public class HermesRestServer {

	private static final Logger logger = LoggerFactory.getLogger(HermesRestServer.class);

	private HttpServer httpServer;

	@Inject
	private ClientEnvironment m_env;

	public HermesRestServer() {
	}

	private URI getBaseURI() throws IOException {
		Properties m_properties = m_env.getGlobalConfig();
		int port = Integer.valueOf(m_properties.getProperty("rest.port"));
		String host = "0.0.0.0";
		URI result = UriBuilder.fromUri("http://" + host).port(port).build();
		return result;
	}

	public void start() throws IOException {
		URI uri = getBaseURI();
		ResourceConfig rc = new ResourceConfig();
		rc.packages("com.ctrip.hermes.rest.resource");
		rc.register(CharsetResponseFilter.class);
		rc.register(CORSResponseFilter.class);
		rc.register(ObjectMapperProvider.class);
		rc.register(MultiPartFeature.class);

		try {
			httpServer = GrizzlyHttpServerFactory.createHttpServer(uri, rc);
			logger.info(String.format("Hermes Rest Server <%s> started.", uri));
		} catch (Throwable e) {
			logger.error("Start Hermes Rest Server error: ", e);
		}

		Properties m_properties = m_env.getGlobalConfig();
		String catUrl = m_properties.getProperty("cat.url", "cat.ctripcorp.com");

		Cat.initializeByDomain(m_properties.getProperty("hermes.rest.appid", "hermes"), 2280, 80, catUrl);

		logger.info("CAT started: URL--{}", catUrl);
	}

	public static void main(String[] args) throws InterruptedException, IOException {
		HermesRestServer hermesRestServer = PlexusComponentLocator.lookup(HermesRestServer.class);
		hermesRestServer.start();

		Thread.currentThread().join();
	}
}
