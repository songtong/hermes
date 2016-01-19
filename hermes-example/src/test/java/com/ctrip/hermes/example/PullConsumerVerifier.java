package com.ctrip.hermes.example;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.junit.Test;
import org.unidal.tuple.Triple;

import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.consumer.api.PullConsumerConfig;
import com.ctrip.hermes.consumer.api.PullConsumerHolder;
import com.ctrip.hermes.consumer.api.PulledBatch;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class PullConsumerVerifier {

	@Test
	public void test() throws Exception {
		String topic = "order_new";
		String groupId = "group1";

		PullConsumerConfig config = new PullConsumerConfig();
		final PullConsumerHolder<String> holder = Consumer.getInstance().openPullConsumer(topic, groupId, String.class,
		      config);

		List<Callable<PulledBatch<String>>> retrives = new ArrayList<>();
		retrives.add(new Callable<PulledBatch<String>>() {

			@Override
			public PulledBatch<String> call() throws Exception {
				return holder.poll(100, 1000);
			}
		});
		retrives.add(new Callable<PulledBatch<String>>() {

			@Override
			public PulledBatch<String> call() throws Exception {
				return holder.collect(100, 1000);
			}
		});

		Verifier v = new Verifier();

		int[] retriveIndexes = new int[] { 0, 1 };
		double[] retriveDis = new double[] { 0.5, 0.5 };
		IntegerDistribution retriveRnd = new EnumeratedIntegerDistribution(retriveIndexes, retriveDis);

		// nack some msg
		int[] nackIndexes = new int[] { 0, 1 };
		double[] nackDis = new double[] { 0.02, 0.98 };
		IntegerDistribution nackRnd = new EnumeratedIntegerDistribution(nackIndexes, nackDis);

		int noMsgCount = 0;
		while (true) {
			PulledBatch<String> batch = retrives.get(retriveRnd.sample()).call();
			List<ConsumerMessage<String>> msgs = batch.getMessages();
			if (msgs.isEmpty()) {
				noMsgCount++;
				if (noMsgCount >= 10) {
					ConcurrentMap<Triple<Integer, Integer, Boolean>, PartitionStatus> status = v.status();
					boolean ok = true;
					for (Entry<Triple<Integer, Integer, Boolean>, PartitionStatus> entry : status.entrySet()) {
						PartitionStatus ps = entry.getValue();
						if (entry.getKey().getMiddle() == Integer.MIN_VALUE) {
							// resend
							if (ps.getToBeNacks().size() != 0) {
								System.out.println(entry);
								ok = false;
							}
						} else {
							if (ps.getCurNonresendOffset() != 10000 || ps.getToBeNacks().size() != 0) {
								System.out.println(entry);
								ok = false;
							}
						}
					}
					if (ok) {
						System.out.println("OK");
						break;
					}
				}
			}
			v.retrived(msgs);

			List<ConsumerMessage<String>> nonpriorityNacked = new LinkedList<>();
			List<ConsumerMessage<String>> priorityNacked = new LinkedList<>();

			for (ConsumerMessage<String> msg : msgs) {
				if (nackRnd.sample() == 0) {
					if (msg.isPriority()) {
						priorityNacked.add(msg);
					} else {
						nonpriorityNacked.add(msg);
					}
				}
			}

			// will receive nonpriority message's resend first
			nonpriorityNacked.addAll(priorityNacked);
			if (!nonpriorityNacked.isEmpty()) {
				v.nacked(nonpriorityNacked);
				for (ConsumerMessage<String> msg : nonpriorityNacked) {
					msg.nack();
				}
			}

			batch.commitAsync();
		}

	}

	class Verifier {

		// partition, priority or resend(Integer.MIN_VALUE, priority)
		private ConcurrentMap<Triple<Integer, Integer, Boolean>, PartitionStatus> statuses = new ConcurrentHashMap<>();

		public void retrived(List<ConsumerMessage<String>> msgs) {
			for (ConsumerMessage<String> msg : msgs) {
				int por = priorityOrResend(msg);
				PartitionStatus status = createOrGetPartitionStatus(msg.getPartition(), por, msg.isPriority());
				status.retrived(msg);
			}
		}

		public void nacked(List<ConsumerMessage<String>> msgs) {
			for (ConsumerMessage<String> msg : msgs) {
				// System.out.println(String.format("NACK %s %s", msg.getPartition(), msg.getBody()));
				PartitionStatus status = createOrGetPartitionStatus(msg.getPartition(), Integer.MIN_VALUE, msg.isPriority());
				status.nacked(msg);
			}
		}

		private PartitionStatus createOrGetPartitionStatus(int partition, int por, boolean isPriority) {
			Triple<Integer, Integer, Boolean> triple = new Triple<>(partition, por, isPriority);
			PartitionStatus status = statuses.get(triple);
			if (status == null) {
				statuses.putIfAbsent(triple, new PartitionStatus());
				status = statuses.get(triple);
			}
			return status;
		}

		private int priorityOrResend(ConsumerMessage<String> msg) {
			return msg.isResend() ? Integer.MIN_VALUE : (msg.isPriority() ? 0 : 1);
		}

		public ConcurrentMap<Triple<Integer, Integer, Boolean>, PartitionStatus> status() {
			return statuses;
		}
	}

	class PartitionStatus {

		private long curNonresendOffset = 0L;

		private BlockingQueue<ConsumerMessage<String>> toBeNacks = new LinkedBlockingQueue<>();

		public void retrived(ConsumerMessage<String> msg) {
			boolean isResend = msg.isResend();

			if (isResend) {
				boolean found = false;
				List<ConsumerMessage<String>> unordered = new ArrayList<>();
				while (!toBeNacks.isEmpty()) {
					ConsumerMessage<String> expectedNack = toBeNacks.poll();
					if (expectedNack.getBody().equals(msg.getBody())) {
						found = true;
					} else {
						unordered.add(expectedNack);
					}
				}

				toBeNacks.addAll(unordered);
				if (!found) {
					System.out.println(unordered);
					System.out.println(String.format("wrong nack %s act:%s", msg.getPartition(), msg.getBody()));
				}
			} else {
				long offset = msg.getOffset();
				if (offset != curNonresendOffset + 1) {
					System.out.println("wrong offset " + offset);
				} else {
					curNonresendOffset = offset;
				}
			}
		}

		public void nacked(ConsumerMessage<String> msg) {
			toBeNacks.add(msg);
		}

		@Override
		public String toString() {
			return "PartitionStatus [curNonresendOffset=" + curNonresendOffset + ", toBeNacks=" + toBeNacks.size() + "]";
		}

		public long getCurNonresendOffset() {
			return curNonresendOffset;
		}

		public BlockingQueue<ConsumerMessage<String>> getToBeNacks() {
			return toBeNacks;
		}

	}
}
