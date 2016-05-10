package com.ctrip.hermes.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.utils.ApplicationContextUtils;

/**
 * @author tenglinxiao
 *
 */
@SpringBootApplication
@EnableScheduling
@EnableAsync
public class Bootstrap extends SpringBootServletInitializer implements ApplicationContextAware, SchedulingConfigurer {

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		ApplicationContextUtils.setApplicationContext(applicationContext);
	}
	
	@Override
	public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
		ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
		CollectorConfiguration conf = ApplicationContextUtils.getBean(CollectorConfiguration.class);
		scheduler.setPoolSize(conf.getSchedulerPoolSize());
		scheduler.initialize();
		taskRegistrar.setTaskScheduler(scheduler);
	}
	
	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(Bootstrap.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(Bootstrap.class);
	}	
}
