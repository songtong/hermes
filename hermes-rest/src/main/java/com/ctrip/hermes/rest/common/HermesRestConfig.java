package com.ctrip.hermes.rest.common;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

public class HermesRestConfig extends ResourceConfig{
	public HermesRestConfig() {
		packages("com.ctrip.hermes.rest.resource");
		register(CharsetResponseFilter.class);
		register(CORSResponseFilter.class);
		register(ObjectMapperProvider.class);
		register(MultiPartFeature.class);
	}
}
