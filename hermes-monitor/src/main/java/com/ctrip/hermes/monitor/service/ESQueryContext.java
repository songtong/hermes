package com.ctrip.hermes.monitor.service;

import java.util.Date;

public class ESQueryContext {

	private String m_index;

	private String m_documentType;

	private String m_keyWord;

	private String m_querySchema;

	private String m_groupSchema;

	private long m_from;

	private long m_to;

	public String getIndex() {
		return m_index;
	}

	public void setIndex(String index) {
		this.m_index = index;
	}

	public String getDocumentType() {
		return m_documentType;
	}

	public void setDocumentType(String docType) {
		this.m_documentType = docType;
	}

	public String getKeyWord() {
		return m_keyWord;
	}

	public void setKeyWord(String keyWord) {
		this.m_keyWord = keyWord;
	}

	public String getQuerySchema() {
		return m_querySchema;
	}

	public void setQuerySchema(String querySchema) {
		this.m_querySchema = querySchema;
	}

	public String getGroupSchema() {
		return m_groupSchema;
	}

	public void setGroupSchema(String groupSchema) {
		this.m_groupSchema = groupSchema;
	}

	public long getFrom() {
		return m_from;
	}

	public void setFrom(long from) {
		this.m_from = from;
	}

	public long getTo() {
		return m_to;
	}

	public void setTo(long to) {
		this.m_to = to;
	}

	@Override
	public String toString() {
		return "ESQueryContext [index=" + m_index + ", documentType=" + m_documentType + ", keyWord=" + m_keyWord
		      + ", querySchema=" + m_querySchema + ", groupSchema=" + m_groupSchema + ", from=" + new Date(m_from)
		      + ", to=" + new Date(m_to) + "]";
	}
}
