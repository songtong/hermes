package com.ctrip.hermes.portal.resource;

import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;

import com.ctrip.hermes.portal.server.CORSResponseFilter;
import com.ctrip.hermes.portal.server.CharsetResponseFilter;
import com.ctrip.hermes.portal.server.ObjectMapperProvider;

public class PortalRestApplication extends ResourceConfig {
	public PortalRestApplication() {
		register(CharsetResponseFilter.class);
		register(CORSResponseFilter.class);
		register(ObjectMapperProvider.class);
		register(MultiPartFeature.class);
		packages("com.ctrip.hermes.portal.resource");
	}
}
