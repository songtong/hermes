package com.ctrip.hermes.collector.pipeline.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Pipeline {	
	// Name for the pipeline.
	public String name() default "";
	
	// Group for the pipeline.
	public String group() default "";
	
	// Order in this named pipeline.
	public int order() default Integer.MAX_VALUE;
}
