package com.ctrip.hermes.monitor.service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.queue.DeadLetter;
import com.ctrip.hermes.metaservice.queue.DeadLetterDao;
import com.ctrip.hermes.metaservice.queue.DeadLetterEntity;
import com.ctrip.hermes.monitor.checker.BaseCheckerTest;
import com.ctrip.hermes.monitor.domain.MonitorItem;
import com.ctrip.hermes.monitor.service.ESMonitorService;
import com.ctrip.hermes.monitor.zabbix.ZabbixConst;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = BaseCheckerTest.class)
public class ESMonitorServiceTest {
	@Autowired
	private ESMonitorService m_es;

	@Test
	public void testQueryBrokerError() {
		long now = System.currentTimeMillis();
		long before = now - TimeUnit.DAYS.toMillis(90);
		Map<String, Long> errors = m_es.queryBrokerErrorCount(before, now);
		for (Entry<String, Long> entry : errors.entrySet()) {
			System.out.println(entry.getKey() + "\t" + entry.getValue());
		}
	}

	@Test
	public void testDao() throws Exception {
		DeadLetterDao dao = PlexusComponentLocator.lookup(DeadLetterDao.class);
		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		Date date = f.parse("2015-11-02 23:17:37");
		DeadLetter d = dao.findByTimeRange("song.test", 0, new Date(0), date, DeadLetterEntity.READSET_COUNT);
		System.out.println(d.getCountOfTimeRange());
	}

	@Test
	public void testQueryLatest() {
		MonitorItem item = m_es.queryLatestMonitorItem(ZabbixConst.CATEGORY_CPU);
		System.out.println(item);
	}
}
