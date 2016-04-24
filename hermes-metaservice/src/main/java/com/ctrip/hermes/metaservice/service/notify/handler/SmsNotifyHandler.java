package com.ctrip.hermes.metaservice.service.notify.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.metaservice.service.notify.HermesNotification;

@Named(type = NotifyHandler.class, value = SmsNotifyHandler.ID)
public class SmsNotifyHandler extends AbstractNotifyHandler {
	private static final Logger log = LoggerFactory.getLogger(SmsNotifyHandler.class);

	public static final String ID = "SmsNotifyHandler";

	@Override
	public void handle(HermesNotification notification) {
		if (notification != null) {
			try {
				persistNotification(notification);
			} catch (Exception e) {
				log.error("Persist sms notification failed, {}", notification, e);
			}
		}
	}

}
