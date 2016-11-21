package com.ctrip.hermes.admin.core.service.mail.assist;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.ctrip.hermes.admin.core.service.template.HermesTemplate;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface HermesMailDescription {
	public HermesTemplate template();

	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@Documented
	public static @interface ContentField {
		public String name() default "";
	}

	@Target(ElementType.FIELD)
	@Retention(RetentionPolicy.RUNTIME)
	@Documented
	public static @interface Subject {
	}

}
