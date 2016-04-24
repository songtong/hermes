package com.ctrip.hermes.metaservice.assist;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.hermes.core.utils.StringUtils;
import com.ctrip.hermes.metaservice.assist.HermesMailDescription.MailContentField;
import com.ctrip.hermes.metaservice.assist.HermesMailDescription.MailTitle;
import com.ctrip.hermes.metaservice.service.template.HermesTemplate;

public class HermesMailUtil {
	private static final Logger log = LoggerFactory.getLogger(HermesMailUtil.class);

	private static final String DEFAULT_HERMES_MAIL_TITLE = "[Hermes] Notification";

	public static HermesMailContext getHermesMailContext(Object obj) {
		Class<? extends Object> clazz = obj.getClass();
		HermesMailDescription annotation = clazz.getAnnotation(HermesMailDescription.class);

		if (annotation == null) {
			throw new IllegalArgumentException("Hermes mail object must specific HermesMail annotation.");
		}

		HermesTemplate template = annotation.template();

		if (template == null) {
			throw new IllegalArgumentException("Hermes mail object must specific HermesTemplate");
		}

		String title = null;
		Map<String, Object> contentMap = new HashMap<>();

		for (Field field : clazz.getDeclaredFields()) {
			title = getTitle4MailIfPossible(field, obj);
			addContent4MailIfPossible(field, obj, contentMap);
		}

		if (StringUtils.isBlank(title)) {
			title = DEFAULT_HERMES_MAIL_TITLE;
		}

		return new HermesMailContext(title, template, contentMap);
	}

	private static String getTitle4MailIfPossible(Field field, Object obj) {
		MailTitle titleField = field.getAnnotation(MailTitle.class);
		if (titleField != null && !StringUtils.isBlank(titleField.title())) {
			try {
				field.setAccessible(true);
				String title = field.get(obj).toString();
				field.setAccessible(false);
				return title;
			} catch (Exception e) {
				log.error("Get hermes mail template information failed: {} {}", obj.getClass(), titleField, e);
			}
		}
		return null;
	}

	private static void addContent4MailIfPossible(Field field, Object obj, Map<String, Object> map) {
		MailContentField mailField = field.getAnnotation(MailContentField.class);
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
