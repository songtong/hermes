package com.ctrip.hermes.rest.service;

import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.rest.common.RestConstant;

@Named
public class CmessageTransferService {
	private static Logger logger = LogManager.getLogger(CmessageTransferService.class);
	private BlockingQueue<Triple<String, String, String>> queue = new LinkedBlockingDeque<>(20000);

	@Inject
	private ClientEnvironment env;

	private Producer producer;

	private String defaultTopic;

	private CmessageTransferService() {
		producer = Producer.getInstance();
		defaultTopic = env.getGlobalConfig().getProperty("cmessage.topic");

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

	private void doSendBatch(ArrayList<Triple<String, String, String>> msgs) {
		for (Triple<String, String, String> msg : msgs) {
			doSend(msg.getFirst(), msg.getMiddle(), msg.getLast());
		}
	}

	private static class ServiceHodler {

		private static CmessageTransferService instance = new CmessageTransferService();
	}

	public static CmessageTransferService getInstance() {
		return ServiceHodler.instance;
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
