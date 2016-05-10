package com.ctrip.hermes.collector.job.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.ctrip.hermes.collector.job.strategy.ExecutionStrategy;
import com.ctrip.hermes.collector.job.strategy.NoRetryExecutionStrategy;


@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface JobStrategy {
	// Strategy for execution.
	Class<? extends ExecutionStrategy> value() default NoRetryExecutionStrategy.class;
	
	// Delay for next retry.
	long retryDelay() default 1000;
	
	// Retry times.
	int retries() default 1;
}
