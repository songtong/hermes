package com.ctrip.hermes.monitor.checker.mysql;

import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.monitor.checker.Checker;
import com.ctrip.hermes.monitor.config.MonitorConfig;
import com.ctrip.hermes.monitor.utils.MonitorUtils;

public abstract class DBBasedChecker implements Checker {
	protected static final int DB_CHECKER_THREAD_COUNT = 10;

	@Autowired
	protected MonitorConfig m_config;

	protected Meta fetchMeta() {
		try {
			return JSON.parseObject(MonitorUtils.curl(m_config.getMetaRestUrl(), 3000, 1000), Meta.class);
		} catch (Exception e) {
			throw new RuntimeException("Fetch meta failed.", e);
		}
	}

}
