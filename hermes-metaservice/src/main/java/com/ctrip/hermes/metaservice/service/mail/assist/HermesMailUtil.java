package com.ctrip.hermes.metaservice.service.mail.assist;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

public class HermesMailUtil {
	private static final Logger log = LoggerFactory.getLogger(HermesMailUtil.class);

	private static final String DEFAULT_HERMES_MAIL_SUBJECT = "[Hermes] Notification";

	public static HermesMailContext getHermesMailContext(MailNoticeContent content) {
		Class<? extends Object> clazz = content.getClass();
		HermesMailDescription annotation = clazz.getAnnotation(HermesMailDescription.class);

		if (annotation == null) {
			throw new IllegalArgumentException("Hermes mail notice content must specific HermesMail annotation.");
		}

		HermesTemplate template = annotation.template();

		if (template == null) {
			throw new IllegalArgumentException("Hermes mail notice content must specific HermesTemplate");
		}

		String subject = StringUtils.isBlank(content.getSubject()) ? DEFAULT_HERMES_MAIL_SUBJECT : content.getSubject();

		Map<String, Object> contentMap = new HashMap<>();
		for (Field field : clazz.getDeclaredFields()) {
			addContent4MailIfPossible(field, content, contentMap);
		}

		return new HermesMailContext(subject, template, contentMap);
	}

	private static void addContent4MailIfPossible(Field field, Object obj, Map<String, Object> map) {
		ContentField mailField = field.getAnnotation(ContentField.class);
		if (mailField != null && !StringUtils.isBlank(mailField.name())) {
			try {
				field.setAccessible(true);
				map.put(mailField.name(), field.get(obj));
				field.setAccessible(false);
			} catch (Exception e) {
				log.error("Get hermes mail template information failed: {} {}", obj.getClass(), mailField, e);
			}
		}
	}
}
