package com.ctrip.hermes.metaservice.service.notify;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.notify.handler.NotifyHandler;

public class DefaultNotifyService implements NotifyService {
	private static final Logger log = LoggerFactory.getLogger(DefaultNotifyService.class);

	@Override
	public boolean notify(HermesNotice notice) {
		if (notice != null && notice.getType() != null) {
			NotifyHandler handler = PlexusComponentLocator.lookup(NotifyHandler.class, notice.getType().handlerID());
			try {
				if (handler != null) {
					return handler.handle(notice);
				}
			} catch (Exception e) {
				log.error("Handle hermes notification failed! {}", notice, e);
			}
		}
		return false;
	}
}
