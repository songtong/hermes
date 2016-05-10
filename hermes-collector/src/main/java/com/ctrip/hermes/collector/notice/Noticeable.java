package com.ctrip.hermes.collector.notice;

import com.ctrip.hermes.collector.exception.NoticeException;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;

/**
 * @author tenglinxiao
 *
 */
public interface Noticeable {
    // Get notice instance.
	HermesNotice get() throws NoticeException;
}
