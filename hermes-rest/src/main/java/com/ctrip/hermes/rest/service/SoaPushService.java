package com.ctrip.hermes.rest.service;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Disposable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.admin.core.view.SubscriptionView;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.core.log.FileBizLogger;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.message.payload.RawMessage;
import com.ctrip.hermes.env.ClientEnvironment;

@Named
public class SoaPushService implements Initializable, Disposable {

	private static final Logger m_logger = LoggerFactory.getLogger(SoaPushService.class);

	@Inject
	private FileBizLogger m_bizLogger;

	@Inject
	private ClientEnvironment m_env;

	@Override
	public void dispose() {
	}

	@Override
	public void initialize() throws InitializationException {

	}

	public ConsumerHolder startPusher(final SubscriptionView sub) {
		final ConsumerHolder consumerHolder = Consumer.getInstance().start(sub.getTopic(), sub.getGroup(),
		      new BaseMessageListener<RawMessage>() {

			      @Override
			      protected void onMessage(final ConsumerMessage<RawMessage> msg) {

			      }
		      });
		return consumerHolder;
	}

}
