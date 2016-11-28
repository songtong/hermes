package com.ctrip.hermes.collector.notice;

import com.ctrip.hermes.admin.core.service.notify.HermesNotice;
import com.ctrip.hermes.collector.exception.NoticeException;

/**
 * @author tenglinxiao
 *
 */
public interface Noticeable {
    // Get notice instance.
	HermesNotice get() throws NoticeException;
}
