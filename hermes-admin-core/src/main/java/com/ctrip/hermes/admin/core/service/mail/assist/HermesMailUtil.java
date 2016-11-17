package com.ctrip.hermes.admin.core.service.mail.assist;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.admin.core.service.mail.assist.HermesMailDescription.Subject;
import com.ctrip.hermes.admin.core.service.notify.MailNoticeContent;
import com.ctrip.hermes.admin.core.service.template.HermesTemplate;
import com.ctrip.hermes.core.utils.StringUtils;

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
			throw new IllegalArgumentException("Hermes mail notice content must specific HermesTemplate.");
		}

		String subject = null;

		Map<String, Object> contentMap = new HashMap<>();
		for (Field field : clazz.getDeclaredFields()) {
			subject = getSubject4MailIfPossible(field, content, subject);
			addContent4MailIfPossible(field, content, contentMap);
		}

		subject = StringUtils.isBlank(subject) ? DEFAULT_HERMES_MAIL_SUBJECT : subject;

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

	private static String getSubject4MailIfPossible(Field field, Object obj, String oldValue) {
		String subject = oldValue;
		Subject mailField = field.getAnnotation(Subject.class);
		if (mailField != null) {
			try {
				field.setAccessible(true);
				subject = (String) field.get(obj);
				field.setAccessible(false);
			} catch (Exception e) {
				log.error("Get hermes mail template information failed: {} {}", obj.getClass(), mailField, e);
			}
		}
		return subject;
	}
}
