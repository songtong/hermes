package com.ctrip.hermes.monitor.dashboard;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.Scheduled;

//@Component
public class DashboardSuite {

	@Autowired
	private RoomStatusConsumeBacklogEmittor m_roomStatusEmittor;

	@PostConstruct
	public void afterPropertiesSet() throws Exception {

	}

	@Scheduled(cron = "0 */1 * * * *")
	public void runSuite() {
		m_roomStatusEmittor.emit();
	}

	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = SpringApplication.run(Bootstrap.class);
		ctx.getBean(DashboardSuite.class).runSuite();
	}

	@SpringBootApplication
	@ComponentScan(basePackages = "com.ctrip.hermes.monitor")
	public static class Bootstrap {
	}

}
