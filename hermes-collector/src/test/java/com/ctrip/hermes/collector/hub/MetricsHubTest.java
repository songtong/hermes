package com.ctrip.hermes.collector.hub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ctrip.hermes.collector.collector.CollectorTest;
import com.ctrip.hickwall.proxy.common.DataPoint;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes=CollectorTest.class)
public class MetricsHubTest {
	@Autowired
	private MetricsHub m_metricsHub;
	
	@Test
	public void test() throws IOException {
		int count = 0;
		while (count++ < 10) {
			DataPoint point = new DataPoint("lxteng.ctrip", 3d, TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()));
			point.setEndpoint("lxteng");
			ArrayList<DataPoint> points = new ArrayList<>();
			points.add(point);
			m_metricsHub.send(points);
		}
		System.in.read();
	}
}
