package com.ctrip.hermes.metaservice.service.notify.handler;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.ShortNoticeContent;
import com.ctrip.soa.common.types.v1.AckCodeType;
import com.ctrip.soa.platform.cti.comm.messageplatfrom.v1.MessagePlatformServiceClient;
import com.ctrip.soa.platform.cti.comm.messageplatfrom.v1.SendMessageRequestType;
import com.ctrip.soa.platform.cti.comm.messageplatfrom.v1.SendMessageResponseType;
import com.ctriposs.baiji.rpc.client.ServiceClientBase;
import com.ctriposs.baiji.rpc.client.ServiceClientConfig;

@Named(type = NotifyHandler.class, value = SmsNotifyHandler.ID)
public class SmsNotifyHandler extends AbstractNotifyHandler implements Initializable {
	private static final Logger log = LoggerFactory.getLogger(SmsNotifyHandler.class);

	private static final long DFT_SMS_LIMIT = 1;

	private static final long DFT_SMS_INTERVAL = 1800000;

	private static final int DEFAULT_SMS_THREAD_COUNT = 1;

	private static final int DEFAULT_SMS_MESSAGE_CODE = 510009;

	private static final long DEFAULT_SMS_EXPIRE_DURATION_MILLI = TimeUnit.MINUTES.toMillis(5);

	private static final String TEST_FX_CFG_URL = "http://ws.config.framework.fws.qa.nt.ctripcorp.com/configws/";

	private static final String UAT_FX_CFG_URL = "http://ws.config.framework.uat.qa.nt.ctripcorp.com/configws/";

	private static final String PROD_FX_CFG_URL = "http://ws.config.framework.ctripcorp.com/configws/";

	public static final SimpleDateFormat DEFAULT_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static final String ID = "SmsNotifyHandler";

	@Inject
	private ClientEnvironment m_env;

	private int m_msgCode = DEFAULT_SMS_MESSAGE_CODE;

	private ExecutorService m_smsExecutor = Executors.newFixedThreadPool(DEFAULT_SMS_THREAD_COUNT);

	protected static class WrappedSms {
		private Map<String, String> m_channelInfo = new HashMap<>();

		private Map<String, String> m_content = new HashMap<>();

		public WrappedSms(String mobile, String content) {
			m_channelInfo.put("MobilePhone", mobile);
			m_channelInfo.put("SmsExpiredTime",
			      DEFAULT_FORMAT.format(new Date(System.currentTimeMillis() + DEFAULT_SMS_EXPIRE_DURATION_MILLI)));
			m_content.put("content", content);
		}

		public Map<String, String> getChannelInfo() {
			return m_channelInfo;
		}

		public Map<String, String> getContent() {
			return m_content;
		}
	}

	@Override
	protected boolean doHandle(boolean persisted, final HermesNotice notice) {
		final AtomicInteger sentCount = new AtomicInteger(0);

		List<String> receivers = notice.getReceivers();

		final String smsContent = ((ShortNoticeContent) notice.getContent()).getContent();

		final CountDownLatch latch = new CountDownLatch(receivers.size());
		for (final String receiver : receivers) {
			m_smsExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						if (doSendSms(smsContent, receiver)) {
							sentCount.getAndIncrement();
						}
					} finally {
						latch.countDown();
					}
				}
			});
		}

		try {
			latch.await(DEFAULT_SMS_EXPIRE_DURATION_MILLI, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			// ignore
		}

		return sentCount.get() > 0;
	}

	private boolean doSendSms(String content, String receiver) {
		try {
			MessagePlatformServiceClient client = MessagePlatformServiceClient.getInstance();
			SendMessageRequestType smrt = new SendMessageRequestType();
			smrt.setMessageCode(m_msgCode);
			smrt.setMsgBody(JSON.toJSONString(new WrappedSms(receiver, content)));

			SendMessageResponseType resp = client.sendMessage(smrt);

			if (resp == null || !resp.getResponseStatus().getAck().equals(AckCodeType.SUCCESS)) {
				log.error("Send sms failed: {}", content);
			} else {
				return true;
			}
		} catch (Exception e) {
			log.error("Send sms failed: {}", content, e);
		}
		return false;
	}

	@Override
	public void initialize() throws InitializationException {
		m_msgCode = Integer.valueOf(m_env.getGlobalConfig().getProperty("notify.handler.sms.msg.code",
		      String.valueOf(DEFAULT_SMS_MESSAGE_CODE)));
		ServiceClientConfig config = new ServiceClientConfig();
		config.setAppId("hermes-metaservice");
		config.setFxConfigServiceUrl(getFxCfgUrl());
		config.setSubEnv(m_env.getEnv().name().toLowerCase());
		ServiceClientBase.initialize(config);
	}

	private String getFxCfgUrl() {
		switch (m_env.getEnv()) {
		case DEV:
		case FAT:
		case FWS:
		case LOCAL:
		case LPT:
			return TEST_FX_CFG_URL;
		case UAT:
			return UAT_FX_CFG_URL;
		case TOOLS:
		case PROD:
			return PROD_FX_CFG_URL;
		default:
			return TEST_FX_CFG_URL;
		}
	}

	@Override
	protected Pair<Long, Long> getThrottleLimit() {
		return new Pair<Long, Long>(DFT_SMS_LIMIT, DFT_SMS_INTERVAL);
	}
}
