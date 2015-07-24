package com.ctrip.hermes.rest.metrics;

import java.util.concurrent.locks.ReentrantLock;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;

public class RestMetricsRegistry {

	private static RestMetricsRegistry INSTANCE;
	
	private MetricRegistry metricRegistry = new MetricRegistry();

	private HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();

	private static ReentrantLock lock = new ReentrantLock();
	
	private RestMetricsRegistry(){
		
	}
	
	public static RestMetricsRegistry getInstance(){
		if(INSTANCE==null){
			try{
				lock.lock();
				if(INSTANCE==null){
					INSTANCE = new RestMetricsRegistry();
				}
			}finally{
				lock.unlock();
			}
		}
		return INSTANCE;
	}
	
	public static void reset(){
		INSTANCE = null;
	}
	
	public MetricRegistry getMetricRegistry(){
		return metricRegistry;
	}
	
	public HealthCheckRegistry getHealthCheckRegistry(){
		return healthCheckRegistry;
	}
}
