package com.ctrip.hermes.collector.notice.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.ctrip.hermes.admin.core.service.notify.HermesNoticeContent;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface NoticeDetail {
	Class<? extends HermesNoticeContent> value();
}