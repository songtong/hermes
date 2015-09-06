package com.ctrip.hermes.kafka.admin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class HermesConsumerPerf {

	private static class ConsumerPerf extends BaseMessageListener<byte[]> {

		private boolean isRunning;

		private Integer threadId;

		@SuppressWarnings("unused")
		private String name;

		private ConsumerPerfConfig config;

		private AtomicLong totalMessagesRead;

		private AtomicLong totalBytesRead;

		private long bytesRead = 0L;

		private long messagesRead = 0L;

		private long startMs = System.currentTimeMillis();

		private long lastReportTime = startMs;

		private long lastBytesRead = 0L;

		private long lastMessagesRead = 0L;

		private CountDownLatch latch;

		public ConsumerPerf(Integer threadId, String name, ConsumerPerfConfig config, AtomicLong totalMessagesRead,
		      AtomicLong totalBytesRead, CountDownLatch latch, String group) {
			this.threadId = threadId;
			this.name = name;
			this.config = config;
			this.totalMessagesRead = totalMessagesRead;
			this.totalBytesRead = totalBytesRead;
			this.isRunning = true;
			this.latch = latch;
		}

		@Override
		protected void onMessage(ConsumerMessage<byte[]> msg) {
			if (this.isRunning && messagesRead < config.numMessages) {
				byte[] event = msg.getBody();
				messagesRead += 1;
				bytesRead += event.length;
				if (messagesRead % config.reportingInterval == 0) {
					printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, lastReportTime,
					      System.currentTimeMillis());
					lastReportTime = System.currentTimeMillis();
					lastMessagesRead = messagesRead;
					lastBytesRead = bytesRead;
				}
			} else {
				totalMessagesRead.addAndGet(messagesRead);
				totalBytesRead.addAndGet(bytesRead);
				if (config.showDetailedStats)
					printMessage(threadId, bytesRead, lastBytesRead, messagesRead, lastMessagesRead, startMs,
					      System.currentTimeMillis());
				this.isRunning = false;
				this.latch.countDown();
			}
		}

		private void printMessage(Integer id, Long bytesRead, Long lastBytesRead, Long messagesRead,
		      Long lastMessagesRead, Long startMs, Long endMs) {
			long elapsedMs = endMs - startMs;
			double totalMBRead = (bytesRead * 1.0) / (1024 * 1024);
			double mbRead = ((bytesRead - lastBytesRead) * 1.0) / (1024 * 1024);
			System.out.println(String.format("%s, %d, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(endMs), id,
			      config.consumerConfig.fetchMessageMaxBytes(), totalMBRead, 1000.0 * (mbRead / elapsedMs), messagesRead,
			      ((messagesRead - lastMessagesRead) / elapsedMs) * 1000.0));
		}
	}

	public static void main(String[] args) throws InterruptedException {
		ConsumerPerfConfig config = new ConsumerPerfConfig(args);
		logger.info("Starting consumer...");
		String topic = config.topic;
		String group = config.consumerConfig.groupId();
		int numThreads = config.numThreads;
		AtomicLong totalMessagesRead = new AtomicLong(0);
		AtomicLong totalBytesRead = new AtomicLong(0);
		CountDownLatch latch = new CountDownLatch(numThreads);

		if (!config.hideHeader) {
			if (!config.showDetailedStats)
				System.out
				      .println("start.time, end.time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
			else
				System.out.println("time, fetch.size, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec");
		}

		Engine engine = Engine.getInstance();

		List<Subscriber> subscribers = new ArrayList<Subscriber>();

		for (int i = 0; i < numThreads; i++) {
			Subscriber s = new Subscriber(topic, group, new ConsumerPerf(i, "kafka-zk-consumer-" + i, config,
			      totalMessagesRead, totalBytesRead, latch, group));
			subscribers.add(s);
		}

		logger.info("Sleeping for 1 second.");
		Thread.sleep(1000);
		logger.info("starting threads");
		long startMs = System.currentTimeMillis();
		System.out.println("Starting consumer...");
		for (Subscriber s : subscribers) {
			engine.start(s);
		}

		latch.await();
		long endMs = System.currentTimeMillis();
		double elapsedSecs = (endMs - startMs - config.consumerConfig.consumerTimeoutMs()) / 1000.0;
		if (!config.showDetailedStats) {
			double totalMBRead = (totalBytesRead.get() * 1.0) / (1024 * 1024);
			System.out.println(String.format("%s, %s, %d, %.4f, %.4f, %d, %.4f", config.dateFormat.format(startMs),
			      config.dateFormat.format(endMs), config.consumerConfig.fetchMessageMaxBytes(), totalMBRead, totalMBRead
			            / elapsedSecs, totalMessagesRead.get(), totalMessagesRead.get() / elapsedSecs));
		}
		System.exit(0);
	}

	private static Logger logger = LoggerFactory.getLogger(HermesConsumerPerf.class);
}