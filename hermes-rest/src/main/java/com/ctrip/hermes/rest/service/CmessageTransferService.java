package com.ctrip.hermes.rest.service;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.rest.common.RestConstant;

@Named
public class CmessageTransferService implements Initializable {

	private static final Logger logger = LoggerFactory.getLogger(CmessageTransferService.class);

	@Override
	public void initialize() throws InitializationException {
		defaultTopic = env.getGlobalConfig().getProperty("cmessage.topic");

		if (null == defaultTopic) {
			defaultTopic = "cmessage_fws";
			logger.error("Cmessage's defaultTopic haven't been set. Initialise that to [cmessage_fws]." +
					  " Set \"cmessage.topic\" in hermes.properties to fix this.");
		}

		producer = Producer.getInstance();

		new Thread(new Runnable() {
			@Override
			public void run() {

				while (true) {
					try {
						ArrayList<Triple<String, String, String>> msgs = new ArrayList<>(1000);
						queue.drainTo(msgs, 1000);

						if (msgs.size() == 0) {
							Thread.sleep(50);
							continue;
						} else {
							doSendBatch(msgs);
							Thread.sleep(10);
						}

//						Triple<String, String, String> msg = queue.take();
//						doSend(msg.getFirst(), msg.getMiddle(), msg.getLast());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	private BlockingQueue<Triple<String, String, String>> queue = new LinkedBlockingDeque<>(20000);

	@Inject
	private ClientEnvironment env;

	private Producer producer;

	private String defaultTopic;

	private void doSendBatch(ArrayList<Triple<String, String, String>> msgs) {
		for (Triple<String, String, String> msg : msgs) {
			doSend(msg.getFirst(), msg.getMiddle(), msg.getLast());
		}
	}

	public void transfer(String topic, String content, String header) {
		boolean isSuccess = queue.offer(new Triple<>(topic, content, header));
		if (!isSuccess) {
			logger.error("CmessageTransferService Queue Is FULL! Queue size: " + queue.size());
		}
	}

	@SuppressWarnings("unused")
	private void doSend(String topic, String content, String header) {
		// 由于开发初期的原因，全部放到RestConstant.CMESSAGEING_TOPIC这个topic下
		Future<SendResult> future = producer.message(defaultTopic, null, content)
				  .addProperty(RestConstant.CMESSAGING_ORIGIN_TOPIC, topic)
				  .addProperty(RestConstant.CMESSAGING_HEADER, header)
				  .send();
	}

	public static void main(String[] args) {
		CmessageTransferService service = new CmessageTransferService();

		service.transfer("order_new", "CmessageTransferService:content", "CmessageTransferService:header");
	}
}
