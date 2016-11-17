package com.ctrip.hermes.admin.core.service.notify.handler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.helper.Files.IO;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.admin.core.service.notify.ShortNoticeContent;
import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.env.ClientEnvironment;
import com.google.common.base.Charsets;

@Named(type = NotifyHandler.class, value = TTSNotifyHandler.ID)
public class TTSNotifyHandler implements NotifyHandler, Initializable {
	private static final Logger log = LoggerFactory.getLogger(TTSNotifyHandler.class);

	private static final String TTS_PDID = "1006";

	private static final int TTS_SEND_TIMEOUT_MILLIS = 30000;

	private static final int DEFAULT_TTS_THREAD_COUNT = 1;

	private static final String DEFAULT_TTS_URL = "http://osg.uat.ops.qa.nt.ctripcorp.com/api/TTSOBS";

	private static final String DEFAULT_TTS_TOKEN_PATH = "/opt/data/hermes/TTS.TOKEN";

	public static final String ID = "TtsNotifyHandler";

	@Inject
	private ClientEnvironment m_env;

	private String m_ttsToken;

	private String m_ttsUrl = DEFAULT_TTS_URL;

	private AtomicLong m_ttsTaskIdSeed = new AtomicLong(System.currentTimeMillis());

	private ExecutorService m_ttsExecutor = Executors.newFixedThreadPool(DEFAULT_TTS_THREAD_COUNT);

	@Override
	public boolean handle(HermesNotice notice) {
		final AtomicInteger sentCount = new AtomicInteger(0);

		List<String> receivers = notice.getReceivers();

		final String ttsContent = ((ShortNoticeContent) notice.getContent()).getContent();

		final CountDownLatch latch = new CountDownLatch(receivers.size());
		for (final String receiver : receivers) {
			m_ttsExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						if (doSendTts(ttsContent, receiver)) {
							sentCount.getAndIncrement();
						}
					} finally {
						latch.countDown();
					}
				}
			});
		}

		try {
			latch.await(TTS_SEND_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// ignore
		}

		return sentCount.get() > 0;
	}

	private boolean doSendTts(String content, String receiver) {
		Map<String, Object> payload = new HashMap<>();
		payload.put("access_token", loadTTSToken());
		payload.put("request_body", JSON.toJSONString(generateTTSRequestBody(content, receiver)));

		OutputStream os = null;
		InputStream is = null;

		try {
			HttpURLConnection conn = (HttpURLConnection) new URL(m_ttsUrl).openConnection();

			conn.setConnectTimeout(TTS_SEND_TIMEOUT_MILLIS);
			conn.setReadTimeout(TTS_SEND_TIMEOUT_MILLIS);
			conn.setRequestMethod("POST");
			conn.addRequestProperty("content-type", "application/json");
			conn.setDoOutput(true);
			conn.connect();

			os = conn.getOutputStream();
			os.write(JSON.toJSONBytes(payload));

			int statusCode = conn.getResponseCode();
			is = conn.getInputStream();
			String response = IO.INSTANCE.readFrom(is, Charsets.UTF_8.name());

			if (statusCode == 200) {
				log.info("Sent tts to: {}, content: {}", receiver, content);
				return true;
			}
			log.error("Send tts failed.(url={}, status={}, response={}).", m_ttsUrl, statusCode, response);
		} catch (Exception e) {
			log.error("Send tts failed.(url={}).", m_ttsUrl, e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (Exception e) {
					// ignore it
				}
			}

			if (os != null) {
				try {
					os.close();
				} catch (Exception e) {
					// ignore it
				}
			}
		}
		return false;
	}

	private Map<String, Object> generateTTSRequestBody(String content, String receiver) {
		Map<String, Object> request = new HashMap<>();
		request.put("TaskID", generateTTSTaskID());
		request.put("pdID", TTS_PDID);
		request.put("phoneNO", receiver);
		request.put("content", content);
		return request;
	}

	private String generateTTSTaskID() {
		String seed = String.valueOf((long) (m_ttsTaskIdSeed.getAndAdd(10) / 10));
		return "TTS" + seed.substring(seed.length() - 12);
	}

	private Object loadTTSToken() {
		try {
			if (m_ttsToken == null) {
				synchronized (this) {
					if (m_ttsToken == null) {
						File f = new File( //
						      m_env.getGlobalConfig().getProperty("notify.handler.tts.token.path", DEFAULT_TTS_TOKEN_PATH));
						for (String line : Files.readAllLines(Paths.get(f.toURI()), Charsets.UTF_8)) {
							if (!StringUtils.isBlank(line)) {
								m_ttsToken = line.trim();
							}
						}
					}
				}
			}
		} catch (IOException e) {
			log.error("Load tts token failed.", e);
			throw new RuntimeException("Can not load tts token!", e);
		}
		return m_ttsToken;
	}

	@Override
	public void initialize() throws InitializationException {
		m_ttsUrl = m_env.getGlobalConfig().getProperty("notify.handler.tts.url", DEFAULT_TTS_URL);
	}
}
