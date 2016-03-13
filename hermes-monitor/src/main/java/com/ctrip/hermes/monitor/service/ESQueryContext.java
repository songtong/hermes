package com.ctrip.hermes.monitor.service;

import java.util.Date;

public class ESQueryContext {

	private String m_index;

	private String m_documentType;

	private String m_keyWord;

	private String m_querySchema;

	private String m_groupSchema;

	private Date m_from;

	private Date m_to;

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

	public Date getFrom() {
		return m_from;
	}

	public void setFrom(Date from) {
		this.m_from = from;
	}

	public Date getTo() {
		return m_to;
	}

	public void setTo(Date to) {
		this.m_to = to;
	}

	@Override
	public String toString() {
		return "ESQueryContext [index=" + m_index + ", documentType=" + m_documentType + ", keyWord=" + m_keyWord
		      + ", querySchema=" + m_querySchema + ", groupSchema=" + m_groupSchema + ", from=" + m_from + ", to=" + m_to
		      + "]";
	}
}
