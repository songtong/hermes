package com.ctrip.hermes.producer;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.metrics.HttpMetricsServer;
import com.ctrip.hermes.producer.api.Producer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class ProducerStressTest extends ComponentTestCase {

	public static void main(String[] args) throws Exception {
		ExecutorService executors = Executors.newFixedThreadPool(5);
		// String topic = "hello_world";
		String topic = "order_new";
		int bodyLength = 1000;
		int count = 10000000;

		HttpMetricsServer server = new HttpMetricsServer("localhost", 9999);
		server.start();

		System.in.read();

		System.out.println("Producer started...");

		AtomicLong sending = new AtomicLong(0);
		final AtomicLong sent = new AtomicLong(0);
		final AtomicLong failed = new AtomicLong(0);

		String body = topic + "--" + generateRandomString(bodyLength);

		Producer.getInstance().message(topic, "prepare-msg", topic + "--prepare-msg").sendSync();

		while (sending.get() < count) {

			long id = sending.incrementAndGet();
			SettableFuture<SendResult> future = (SettableFuture<SendResult>) Producer.getInstance()
			      .message(topic, body, body).withRefKey(Long.toString(id)).send();

			Futures.addCallback(future, new FutureCallback<SendResult>() {

				@Override
				public void onSuccess(SendResult result) {
					sent.incrementAndGet();
				}

				@Override
				public void onFailure(Throwable t) {
					failed.incrementAndGet();
				}
			}, executors);

			long current = sending.get();

			if (current != 0 && current % 10000 == 0) {
				System.out.println("Sending: " + sending.get() + " Sent: " + sent.get() + " Failed: " + failed.get());
				Thread.sleep(20 * 1000);
			}
		}
	}

	private static String generateRandomString(int size) {
		StringBuilder sb = new StringBuilder(size);
		Random random = new Random();
		for (int i = 0; i < size; i++) {
			sb.append('a' + random.nextInt(25));
		}

		return sb.toString();
	}

}
