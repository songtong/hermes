package com.ctrip.hermes.rest.service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
public class CmessageTransferService {

	@Inject
	private ClientEnvironment env;

	private static Logger logger = LoggerFactory.getLogger(CmessageTransferService.class);

	private Producer producer;

	private String defaultTopic;
	
	private BlockingQueue<Triple<String, String, String>> queue = new LinkedBlockingDeque<>(5000);

	private CmessageTransferService() {
		producer = Producer.getInstance();
		defaultTopic = env.getGlobalConfig().getProperty("cmessage.topic");

		new Thread(new Runnable() {
			@Override
			public void run() {

				while (true) {
					try {
						Triple<String, String, String> msg = queue.take();
						doSend(msg.getFirst(), msg.getMiddle(), msg.getLast());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();

	}

	private static class ServiceHodler {

		private static CmessageTransferService instance = new CmessageTransferService();
	}

	public static CmessageTransferService getInstance() {
		return ServiceHodler.instance;
	}

	public void transfer(String topic, String content, String header) {
		queue.offer(new Triple<>(topic, content, header));
	}

	@SuppressWarnings("unused")
	private void doSend(String topic, String content, String header) {
		// 由于开发初期的原因，全部放到RestConstant.CMESSAGEING_TOPIC这个topic下
		Future<SendResult> future = producer.message(defaultTopic, null, content)
		      .addProperty(RestConstant.CMESSAGING_ORIGIN_TOPIC, topic)
		      .addProperty(RestConstant.CMESSAGING_HEADER, header).send();

		SendResult result = null;
		try {
			result = future.get(2, TimeUnit.SECONDS);

			logger.debug(String.format("SendTopic: [%s], Content: [%s]", topic, content));
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("FailToGetMessageFuture: " + e.getMessage());
		}
	}

	public static void main(String[] args) {
		CmessageTransferService service = new CmessageTransferService();

		service.transfer("order_new", "CmessageTransferService:content", "CmessageTransferService:header");
	}
}
