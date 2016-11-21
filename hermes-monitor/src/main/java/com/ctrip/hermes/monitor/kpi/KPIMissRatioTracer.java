package com.ctrip.hermes.monitor.kpi;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.HermesNoticeContent;
import com.ctrip.hermes.admin.core.service.notify.NotifyService;
import com.ctrip.hermes.admin.core.service.notify.TtsNoticeContent;
import com.ctrip.hermes.consumer.api.BaseMessageListener;
import com.ctrip.hermes.consumer.api.Consumer;
import com.ctrip.hermes.core.constants.CatConstants;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.producer.api.Producer;
import com.dianping.cat.Cat;
import com.dianping.cat.message.Transaction;

@Component
public class KPIMissRatioTracer {
	private static final Logger log = LoggerFactory.getLogger(KPIMissRatioTracer.class);

	private static final String DEFAULT_ON_CALL_MOBILE = "13310027139";

	@Autowired
	protected MonitorConfig m_config;

	private NotifyService m_notifyService = PlexusComponentLocator.lookup(NotifyService.class);

	private Set<Integer> m_ttsHours;

	private static Date m_latestConsume = new Date(0);

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
			m_latestConsume = new Date();
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

	private boolean shouldTts() {
		Calendar cal = Calendar.getInstance();
		cal.setTime(new Date());
		return m_ttsHours.contains(cal.get(Calendar.HOUR_OF_DAY));
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
						TimeUnit.SECONDS.sleep(10);
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
		if (m_config.isMonitorCheckerNotifyEnable()) {
			m_ttsHours = new HashSet<>();
			int startHour = m_config.getKpiTtsStartHour();
			if (startHour >= 24) {
				throw new RuntimeException("TTS start hour is too large(more than 24)");
			}
			for (int i = 0; i < m_config.getKpiTtsLastHours() % 24; i++) {
				m_ttsHours.add((startHour + i) % 24);
			}
			Thread t = new Thread(new KPITracerTask(), "HERMES_KPI_TRACER");
			t.setDaemon(true);
			t.start();

			Executors.newSingleThreadExecutor().execute(new Runnable() {
				@Override
				public void run() {
					while (!Thread.interrupted()) {
						Date now = new Date();
						try {
							long freezeMins = m_latestConsume.getTime() != 0 ? TimeUnit.MILLISECONDS.toMinutes(now.getTime()
							      - m_latestConsume.getTime()) : -1;
							if (freezeMins > 0 && freezeMins > m_config.getKpiTtsErrorLimitMinute()) {
								if (shouldTts()) {
									HermesNoticeContent content = new TtsNoticeContent(String.format("紧急情况, Hermes监控超过%s分钟没有消费",
									      freezeMins));
									m_notifyService.notify(new HermesNotice(DEFAULT_ON_CALL_MOBILE, content));
								}
								log.warn("*** *** KPI tracer's consumption stopped for {} minutes...", freezeMins);
							}
						} catch (Exception e) {
							log.error("TTS watch dog failed.", e);
						}
						try {
							TimeUnit.SECONDS.sleep(30);
						} catch (InterruptedException e) {
							Thread.interrupted();
						}
					}
				}
			});
		}
	}
}
