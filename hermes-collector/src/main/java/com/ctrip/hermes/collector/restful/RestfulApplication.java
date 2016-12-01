package com.ctrip.hermes.collector.restful;

import javax.ws.rs.ApplicationPath;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.restful.utils.CharsetResponseFilter;
import com.ctrip.hermes.collector.restful.utils.ObjectMapperProvider;

@Component
@ApplicationPath("/api")
public class RestfulApplication extends ResourceConfig {
	public RestfulApplication() {
		register(CharsetResponseFilter.class);
		register(ObjectMapperProvider.class);
		register(MultiPartFeature.class);
		packages(RestfulApplication.class.getPackage().getName());
	}
}