package com.ctrip.hermes.monitor.kpi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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

	private static class KPIMessageListener extends BaseMessageListener<KPIMessage> {
		long m_idx;

		public KPIMessageListener(long initIdx) {
			m_idx = initIdx;
		}

		@Override
		protected void onMessage(ConsumerMessage<KPIMessage> hermesMsg) {
			KPIMessage msg = hermesMsg.getBody();
			try {
				if (m_idx >= 0 && msg.getIndex() > m_idx) {
					if (msg.getIndex() - m_idx == 1) {
						trace(true);
					} else {
						for (long i = m_idx; i < msg.getIndex() - 1; i++) {
							trace(false);
						}
					}
				}
			} catch (Exception e) {
				log.error("Error happened when trace hermes-kpi.", e);
			} finally {
				m_idx = Math.max(msg.getIndex(), m_idx);
				try {
					persistOffset(KPIConstants.CONSUME_OFFSET_FILE, m_idx);
				} catch (IOException e) {
					log.error("Persist consume offset failed: {}", m_idx);
				}
			}
		}

		private void trace(boolean success) {
			Transaction tx = Cat.newTransaction(CatConstants.TYPE_MESSAGE_MISS_RATIO, "HERMES");
			tx.setStatus(success ? Transaction.SUCCESS : "MISSED");
			tx.complete();
		}
	}

	private static class KPITracerTask implements Runnable {

		@Override
		public void run() {
			log.info("****************  Starting Hermes KPI Tracer Task  *********************");
			long initOffset = -1;
			try {
				initOffset = loadOffset(KPIConstants.CONSUME_OFFSET_FILE);
			} catch (IOException e) {
				throw new RuntimeException("Load kpi_offset file failed. Please check it.");
			}

			Consumer.getInstance().start(KPIConstants.TOPIC, KPIConstants.CONSUMER, new KPIMessageListener(initOffset));

			long producerMsgIdx = -1;
			try {
				producerMsgIdx = loadOffset(KPIConstants.PRODUCE_OFFSET_FILE);
			} catch (IOException e) {
				throw new RuntimeException("Load kpi_offset file failed. Please check it.");
			}
			while (!Thread.interrupted()) {
				try {
					Producer.getInstance().message(KPIConstants.TOPIC, "PK-NONSENSE", new KPIMessage(producerMsgIdx++))
					      .send();
					persistOffset(KPIConstants.PRODUCE_OFFSET_FILE, producerMsgIdx);
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

	private static long loadOffset(File file) throws IOException {
		if (!file.exists()) {
			file.createNewFile();
			persistOffset(file, -1);
		}

		BufferedReader r = new BufferedReader(new FileReader(file));
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

	private static void persistOffset(File file, long producerMsgIdx) throws IOException {
		BufferedWriter w = new BufferedWriter(new FileWriter(file));
		try {
			w.write(String.valueOf(producerMsgIdx));
		} catch (Exception e) {
			log.error("Persist KPI_OFFSET failed.", e);
		} finally {
			if (w != null) {
				w.flush();
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
