package com.ctrip.hermes.collector.job.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.ctrip.hermes.collector.job.JobGroup;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface Job {
	public String name() default "";
	public JobGroup group() default JobGroup.DEFAULT;
}
