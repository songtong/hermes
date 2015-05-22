package com.ctrip.hermes.consumer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.Consumer.ConsumerHolder;
import com.ctrip.hermes.consumer.api.MessageListener;
import com.ctrip.hermes.core.message.ConsumerMessage;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StartConsumer extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		String topic = "order_new";

		Map<String, List<String>> subscribers = new HashMap<String, List<String>>();
		subscribers.put("group1", Arrays.asList("1-a"));
		subscribers.put("group2", Arrays.asList("2-a"));
		// subscribers.put("group2", Arrays.asList("2-a", "2-b"));

		List<ConsumerHolder> holders = new ArrayList<>();

		for (Map.Entry<String, List<String>> entry : subscribers.entrySet()) {
			String groupId = entry.getKey();
			for (String id : entry.getValue()) {

				holders.add(Consumer.getInstance().start(topic, groupId, new MyConsumer(id)));
				System.out.println("Starting consumer " + groupId + ":" + id);
			}

		}

		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			String line = in.readLine();
			if ("c".equals(line)) {
				for (ConsumerHolder holder : holders) {
					holder.close();
				}

				break;
			} else {
				System.out.println("Print Threads: ");
				printAllThreads();
			}
		}

		while (true) {
			String line = in.readLine();
			if ("q".equals(line)) {
				break;
			} else {
				printAllThreads();
			}
		}

		System.in.read();

	}

	private void printAllThreads() {
		Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
		Map<String, List<String>> groups = new HashMap<>();
		for (Thread thread : allStackTraces.keySet()) {
			String group = thread.getThreadGroup().getName();
			if (!groups.containsKey(group)) {
				groups.put(group, new ArrayList<String>());
			}
			groups.get(group).add(thread.getName());
		}

		for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
			System.out.println(String.format("Thread Group %s: threads: %s", entry.getKey(), entry.getValue()));
		}
	}

	static class MyConsumer implements MessageListener<String> {

		private String m_id;

		public MyConsumer(String id) {
			m_id = id;
		}

		@Override
		public void onMessage(List<ConsumerMessage<String>> msgs) {
			for (ConsumerMessage<String> msg : msgs) {
				String body = msg.getBody();
				System.out.println(m_id + "<<< " + body);

				msg.ack();
			}
		}
	}

}
