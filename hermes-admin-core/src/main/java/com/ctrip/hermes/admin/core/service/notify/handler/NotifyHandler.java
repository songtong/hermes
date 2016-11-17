package com.ctrip.hermes.admin.core.service.notify.handler;

import com.ctrip.hermes.admin.core.service.notify.HermesNotice;

public interface NotifyHandler {
	public boolean handle(HermesNotice notice);
}
