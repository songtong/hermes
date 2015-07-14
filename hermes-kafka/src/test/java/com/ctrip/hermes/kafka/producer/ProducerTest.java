package com.ctrip.hermes.kafka.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;

import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.ctrip.hermes.producer.api.Producer.MessageHolder;

public class ProducerTest {

	@Test
	public void kafkaThroughBrokerTest() throws IOException, InterruptedException, ExecutionException {
		String topic = "kafka.SimpleTextTopic";

		Producer producer = Producer.getInstance();

		try (BufferedReader in = new BufferedReader(new InputStreamReader(System.in))) {
			while (true) {
				String line = in.readLine();
				if ("q".equals(line)) {
					break;
				}

				for (int i = 0; i < 100; i++) {
					String proMsg = "Hello Ctrip " + System.currentTimeMillis();
					MessageHolder holder = producer.message(topic, null, proMsg);
					Future<SendResult> future = holder.send();
					future.get();
					System.out.println("Sent: " + proMsg);
				}
			}
		}

	}
}
