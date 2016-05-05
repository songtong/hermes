package com.ctrip.hermes.monitor.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.unidal.tuple.Pair;

public class ESQueryContext {

	private String m_index;

	private String m_documentType;

	private String m_groupSchema;

	private List<Pair<String, String>> m_querys = new ArrayList<>();

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

	public List<Pair<String, String>> getQuerys() {
		return m_querys;
	}

	public void setQuerys(List<Pair<String, String>> querys) {
		m_querys = querys;
	}

	public void addQuery(String key, String value) {
		m_querys.add(new Pair<String, String>(key, value));
	}
}
