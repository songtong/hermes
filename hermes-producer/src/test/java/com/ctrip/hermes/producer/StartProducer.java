package com.ctrip.hermes.producer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StartProducer extends ComponentTestCase {
	private ExecutorService m_executors = Executors.newFixedThreadPool(5);

	@Test
	public void test() throws Exception {
		String topic = "order_new";
		System.out.println(String.format("Starting producer(topic=%s)...", topic));

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
			if ("q".equals(line)) {
				break;
			} else {
				send(topic);
			}

		}
	}

	private void send(String topic) throws Exception {
		String uuid = UUID.randomUUID().toString();
		Random random = new Random();

		boolean priority = random.nextBoolean();
		final String msg = uuid + (priority ? " priority" : " non-priority");
		System.out.println(">>> " + msg);
		SettableFuture<SendResult> future = null;
		if (priority) {
			future = (SettableFuture<SendResult>) Producer.getInstance().message(topic, uuid, msg).withRefKey(uuid).withPriority().send();
		} else {
			future = (SettableFuture<SendResult>) Producer.getInstance().message(topic, uuid, msg).withRefKey(uuid).send();
		}

		Futures.addCallback(future, new FutureCallback<SendResult>() {

			@Override
			public void onSuccess(SendResult result) {
				System.out.println("Message " + msg + " sent.");
			}

			@Override
			public void onFailure(Throwable t) {
				System.out.println("Message " + msg + " sent.");

			}
		}, m_executors);
	}
}
