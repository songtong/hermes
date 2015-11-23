package com.ctrip.hermes.portal.application;

import com.ctrip.hermes.core.utils.StringUtils;

public enum HermesApplicationType {
	CREATE_TOPIC(0, "create_topic_application", TopicApplication.class),
	CREATE_CONSUMER(1,"create_consumer_application",ConsumerApplication.class);

	private int m_typeCode;

	private String m_displayName;

	private Class<? extends HermesApplication> m_clazz;

	private HermesApplicationType(int typeCode, String displayName, Class<? extends HermesApplication> clazz) {
		m_typeCode = typeCode;
		m_displayName = displayName;
		m_clazz = clazz;
	}

	public int getTypeCode() {
		return m_typeCode;
	}

	public String getDisplayName() {
		return StringUtils.isBlank(m_displayName) ? name() : m_displayName;
	}

	public Class<? extends HermesApplication> getClazz() {
		return m_clazz;
	}

	public static HermesApplicationType findByTypeCode(int typeCode) {
		for (HermesApplicationType appType : HermesApplicationType.values()) {
			if (typeCode == appType.getTypeCode()) {
				return appType;
			}
		}
		return null;

	}
}
