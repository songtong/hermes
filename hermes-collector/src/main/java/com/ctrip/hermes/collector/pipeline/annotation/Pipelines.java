package com.ctrip.hermes.collector.pipeline.annotation;

public @interface Pipelines {
	// Pipeline declaration for roles in different pipelines.
	Pipeline[] value() default {};
}
