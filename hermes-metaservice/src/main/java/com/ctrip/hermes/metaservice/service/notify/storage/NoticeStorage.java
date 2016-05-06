package com.ctrip.hermes.metaservice.service.notify.storage;

import java.util.List;

import com.ctrip.hermes.metaservice.service.notify.HermesNotice;

public interface NoticeStorage {
	public List<HermesNotice> findSmsNotices(boolean queryOnly);
}
