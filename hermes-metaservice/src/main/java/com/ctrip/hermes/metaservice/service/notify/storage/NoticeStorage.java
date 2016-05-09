package com.ctrip.hermes.metaservice.service.notify.storage;

import java.util.Date;
import java.util.List;

import com.ctrip.hermes.metaservice.service.notify.HermesNotice;

public interface NoticeStorage {
	public String addNotice(HermesNotice notice) throws Exception;

	public void updateNotifyTime(String refKey, Date date) throws Exception;

	public List<HermesNotice> findSmsNotices(boolean queryOnly);
}
