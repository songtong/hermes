package com.ctrip.hermes.monitor.kpi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.producer.api.Producer;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

@Component
public class KPIMissRatioTracer {
	private static final Logger log = LoggerFactory.getLogger(KPIMissRatioTracer.class);

	private static class KPIMessage {
		private long m_idx;

		@SuppressWarnings("unused")
		public KPIMessage() {
		}

		public KPIMessage(long idx) {
			m_idx = idx;
		}

		public long getIndex() {
			return m_idx;
		}

		@SuppressWarnings("unused")
		public void setIndex(long idx) {
			m_idx = idx;
		}
	}

	private static class KPITracerTask implements Runnable {

		@Override
		public void run() {
			log.info("****************  Starting Hermes KPI Tracer Task  *********************");
			Consumer.getInstance().start(KPIConstants.TOPIC, KPIConstants.CONSUMER, new BaseMessageListener<KPIMessage>() {
				private long lastIdx = -1;

				private void trace(boolean success) {
					Transaction tx = Cat.newTransaction(CatConstants.TYPE_MESSAGE_MISS_RATIO, "HERMES");
					tx.setStatus(success ? Transaction.SUCCESS : "MISSED");
					tx.complete();
				}

				@Override
				protected void onMessage(ConsumerMessage<KPIMessage> hermesMsg) {
					KPIMessage msg = hermesMsg.getBody();
					try {
						if (lastIdx >= 0 && msg.getIndex() > lastIdx) {
							if (msg.getIndex() - lastIdx == 1) {
								trace(true);
							} else {
								for (long i = lastIdx; i < msg.getIndex() - 1; i++) {
									trace(false);
								}
							}
						}
					} catch (Exception e) {
						log.error("Error happened when trace hermes-kpi.", e);
					} finally {
						lastIdx = Math.max(msg.getIndex(), lastIdx);
					}
				}
			});

			long producerMsgIdx = -1;
			try {
				producerMsgIdx = loadProducerOffset();
			} catch (IOException e) {
				throw new RuntimeException("Load kpi_offset file failed. Please check it.");
			}
			while (!Thread.interrupted()) {
				try {
					Producer.getInstance().message(KPIConstants.TOPIC, "PK-NONSENSE", new KPIMessage(producerMsgIdx++)).send();
					persistProduceOffset(producerMsgIdx);
				} catch (Exception e) {
					log.error("Producer KPIMessage for kpi-tracer failed, idx: " + producerMsgIdx);
				} finally {
					try {
						TimeUnit.SECONDS.sleep(1);
					} catch (InterruptedException e) {
						// ignore
					}
				}
			}
		}
	}

	private static long loadProducerOffset() throws IOException {
		if (!KPIConstants.OFFSET_FILE.exists()) {
			KPIConstants.OFFSET_FILE.createNewFile();
			persistProduceOffset(-1);
		}

		BufferedReader r = new BufferedReader(new FileReader(KPIConstants.OFFSET_FILE));
		try {
			return Long.valueOf(r.readLine());
		} catch (Exception e) {
			log.error("Load KPI_OFFSET failed.", e);
		} finally {
			if (r != null) {
				r.close();
			}
		}
		return -1;
	}

	private static void persistProduceOffset(long producerMsgIdx) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(KPIConstants.OFFSET_FILE));
		try {
			w.write(String.valueOf(producerMsgIdx));
		} catch (Exception e) {
			log.error("Persist KPI_OFFSET failed.", e);
		} finally {
			if (w != null) {
				w.close();
			}
		}
	}

	@PostConstruct
	public void startTraceMissRatio() throws Exception {
		Thread t = new Thread(new KPITracerTask(), "HERMES_KPI_TRACER");
		t.setDaemon(true);
		t.start();
	}
}
