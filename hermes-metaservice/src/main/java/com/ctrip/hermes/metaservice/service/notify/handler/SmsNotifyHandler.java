package com.ctrip.hermes.metaservice.service.notify.handler;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.service.notify.HermesNotice;

@Named(type = NotifyHandler.class, value = SmsNotifyHandler.ID)
public class SmsNotifyHandler extends AbstractNotifyHandler {
	private static final int DEFAULT_NOTIFY_INTERVAL = 60;

	public static final String ID = "SmsNotifyHandler";

	@Override
	protected boolean doHandle(boolean persisted, HermesNotice notice) {
		return persisted;
	}

	@Override
	protected int getNotifyIntervalMinute() {
		return DEFAULT_NOTIFY_INTERVAL;
	}

}
