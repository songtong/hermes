package com.ctrip.hermes.monitor;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Component
public class ScheduleTasks {

	@Scheduled(fixedRate = 1000)
	public void test() {

		System.out.println("hello3");
	}
	
	@Scheduled(fixedRate = 2000)
	public void test2() {

		System.out.println("hello4");
	}
}
