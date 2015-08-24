package com.ctrip.hermes.consumer.engine.bootstrap;

import java.util.Arrays;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.meta.entity.Topic;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ConsumerBootstrapManager.class)
public class DefaultConsumerBootstrapManager implements ConsumerBootstrapManager {

	@Inject
	private ConsumerBootstrapRegistry m_registry;

	public ConsumerBootstrap findConsumerBootStrap(Topic topic) {
		if (Storage.KAFKA.equals(topic.getStorageType())) {
			return m_registry.findConsumerBootstrap(Endpoint.KAFKA);
		} else if (Arrays.asList(Endpoint.BROKER, Endpoint.KAFKA).contains(topic.getEndpointType())) {
			return m_registry.findConsumerBootstrap(topic.getEndpointType());
		} else {
			throw new IllegalArgumentException(String.format("Unknown endpoint type: %s", topic.getEndpointType()));
		}
	}

}
