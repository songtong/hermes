package com.ctrip.hermes.consumer.engine;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrap;
import com.ctrip.hermes.consumer.engine.bootstrap.ConsumerBootstrapManager;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.utils.CollectionUtil;
import com.ctrip.hermes.core.utils.CollectionUtil.Transformer;
import com.ctrip.hermes.meta.entity.Topic;

@Named(type = Engine.class)
public class DefaultEngine extends Engine {

	private static final Logger log = LoggerFactory.getLogger(DefaultEngine.class);

	@Inject
	private ConsumerBootstrapManager m_consumerManager;

	@Inject
	private MetaService m_metaService;

	@Override
	public SubscribeHandle start(List<Subscriber> subscribers) {
		CompositeSubscribeHandle handle = new CompositeSubscribeHandle();

		for (Subscriber s : subscribers) {
			List<Topic> topics = m_metaService.listTopicsByPattern(s.getTopicPattern());

			if (topics != null && !topics.isEmpty()) {
				log.info("Found topics({}) matching pattern({}), groupId={}.",
				      CollectionUtil.collect(topics, new Transformer() {

					      @Override
					      public Object transform(Object topic) {
						      return ((Topic) topic).getName();
					      }
				      }), s.getTopicPattern(), s.getGroupId());

				for (Topic topic : topics) {
					ConsumerContext context = new ConsumerContext(topic, s.getGroupId(), s.getConsumer(),
					      s.getMessageClass(), s.getConsumerType());
					try {
						String endpointType = m_metaService.findEndpointTypeByTopic(topic.getName());
						ConsumerBootstrap consumerBootstrap = m_consumerManager.findConsumerBootStrap(endpointType);
						handle.addSubscribeHandle(consumerBootstrap.start(context));

					} catch (Exception e) {
						log.error("Failed to start consumer for topic {}(consumer: groupId={}, sessionId={})",
						      topic.getName(), context.getGroupId(), context.getSessionId(), e);
					}
				}
			} else {
				log.error("Can not find any topics matching pattern {}", s.getTopicPattern());
			}
		}

		return handle;
	}
}
