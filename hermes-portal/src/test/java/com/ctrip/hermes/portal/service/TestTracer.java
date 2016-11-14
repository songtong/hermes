package com.ctrip.hermes.portal.service;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;
import java.util.List;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.junit.Before;
import org.junit.Test;
import org.unidal.helper.Reflects;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.ctrip.env.DefaultClientEnvironment;
import com.ctrip.hermes.meta.entity.ConsumerGroup;
import com.ctrip.hermes.meta.entity.Topic;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.portal.config.PortalConfig;
import com.ctrip.hermes.portal.messageLifecycle.entity.MessageLifecycle;

public class TestTracer {
	private TracerService tracerService;

	@Before
	public void init() throws InitializationException {
		tracerService = new TracerService();

		TopicService mockTopicService = mock(TopicService.class);
		Topic topic = new Topic("myTopicName");
		ConsumerGroup consumer = new ConsumerGroup("myConsumerName");
		topic.setId(901286L);
		consumer.setId(790);
		topic.getConsumerGroups().add(consumer);
		when(mockTopicService.findTopicEntityByName(anyString())).thenReturn(topic);

		Reflects.forField().setDeclaredFieldValue(TracerService.class, "m_topicService", tracerService, mockTopicService);

		TracerEsQueryService esService = new TracerEsQueryService();
		PortalConfig portalConfig = new PortalConfig();
		DefaultClientEnvironment clientEnv = new DefaultClientEnvironment();
		clientEnv.initialize();
		Reflects.forField().setDeclaredFieldValue(PortalConfig.class, "m_env", portalConfig, clientEnv);
		Reflects.forField().setDeclaredFieldValue(TracerEsQueryService.class, "m_config", esService, portalConfig);
		Reflects.forField().setDeclaredFieldValue(TracerService.class, "m_esService", tracerService, esService);
	}

	@Test
	public void test() {
		String refKey = "d8ed588b-461d-4d29-8814-fae7078a3f2f";
		Pair<List<MessageLifecycle>, List<Pair<String, Object>>> trace = tracerService.trace(
		      "sys.cdportal.docker.deleted", new Date(1467648000000L), refKey);

		for (MessageLifecycle mlc : trace.getKey()) {
			System.out.println(mlc);
		}
	}
}
