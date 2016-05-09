package com.ctrip.hermes.metaservice.service.notify.handler;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.service.notify.HermesNotice;

@Named(type = NotifyHandler.class, value = SmsNotifyHandler.ID)
public class SmsNotifyHandler extends AbstractNotifyHandler {

	public static final String ID = "SmsNotifyHandler";

	@Override
	protected boolean doHandle(boolean persisted, HermesNotice notice) {
		return persisted;
	}

}
