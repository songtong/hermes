package com.ctrip.hermes.admin.core.service;

import java.util.Map;

public interface KVService {
	public static enum Tag {
		DEFAULT("dft"), CHECKER("ckr"), CMSG("cmsg");

		private String m_simple;

		private Tag(String simple) {
			m_simple = simple;
		}

		public String simple() {
			return m_simple;
		}

		public static Tag getBySimpleName(String simpleName) {
			for (Tag tag : values()) {
				if (tag.simple().equals(simpleName)) {
					return tag;
				}
			}
			return Tag.DEFAULT;
		}

		public static Tag getByName(String name) {
			try {
				return valueOf(name);
			} catch (Exception e) {
				return DEFAULT;
			}
		}
	}

	public String getValue(String key);

	public String getValue(String key, Tag tag);

	public Map<Tag, Map<String, String>> list();

	public Map<String, String> list(Tag tag);

	public void setKeyValue(String key, String value) throws Exception;

	public void setKeyValue(String key, String value, Tag tag) throws Exception;
}
