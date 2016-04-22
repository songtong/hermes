package com.ctrip.hermes.portal.application;

import java.util.Date;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.portal.dal.application.Application;

public abstract class HermesApplication {
	private long m_id;

	private String m_comment;

	private int m_type;

	private int m_status;

	private String m_content;
	
	private String m_approver;

	private Date m_createTime;

	private String m_ownerEmail1;

	private String m_ownerEmail2;

	private Date m_lastModifiedTime;

	public long getId() {
		return m_id;
	}

	public void setId(long id) {
		this.m_id = id;
	}

	public String getComment() {
		return m_comment;
	}

	public void setComment(String comment) {
		this.m_comment = comment;
	}

	public int getType() {
		return m_type;
	}

	public void setType(int type) {
		this.m_type = type;
	}

	public int getStatus() {
		return m_status;
	}

	public void setStatus(int status) {
		this.m_status = status;
	}

	public String getContent() {
		return m_content;
	}

	public void setContent(String content) {
		this.m_content = content;
	}

	public String getApprover() {
		return m_approver;
	}

	public void setApprover(String approver) {
		this.m_approver = approver;
	}

	public Date getCreateTime() {
		return m_createTime;
	}

	public void setCreateTime(Date createTime) {
		this.m_createTime = createTime;
	}

	public Date getLastModifiedTime() {
		return m_lastModifiedTime;
	}

	public void setLastModifiedTime(Date lastModifiedTime) {
		this.m_lastModifiedTime = lastModifiedTime;
	}

	public static Application toDBEntity(HermesApplication app) {
		Application dbApp = new Application();
		dbApp.setId(app.getId());
		dbApp.setType(app.getType());
		dbApp.setStatus(app.getStatus());
		dbApp.setContent(app.getContent());
		dbApp.setComment(app.getComment());
		dbApp.setOwner1(app.getOwnerEmail1());
		dbApp.setOwner2(app.getOwnerEmail2());
		dbApp.setApprover(app.getApprover());
		dbApp.setCreateTime(app.getCreateTime());
		return dbApp;
	};

	public String getOwnerEmail1() {
		return m_ownerEmail1;
	}

	public void setOwnerEmail1(String ownerEmail1) {
		this.m_ownerEmail1 = ownerEmail1;
	}

	public String getOwnerEmail2() {
		return m_ownerEmail2;
	}

	public void setOwnerEmail2(String ownerEmail2) {
		this.m_ownerEmail2 = ownerEmail2;
	}

	public static HermesApplication parse(Application dbApp) {
		// except for content value
		HermesApplicationType type = HermesApplicationType.findByTypeCode(dbApp.getType());
		HermesApplication app = JSON.parseObject(dbApp.getContent(), type.getClazz());
		app.setType(dbApp.getType());
		app.setId(dbApp.getId());
		app.setApprover(dbApp.getApprover());
		app.setComment(dbApp.getComment());
		app.setCreateTime(dbApp.getCreateTime());
		app.setStatus(dbApp.getStatus());
		app.setLastModifiedTime(dbApp.getDataChangeLastTime());
		return app;

	}

	@Override
	public String toString() {
		return "HermesApplication [m_id=" + m_id + ", m_comment=" + m_comment + ", m_type=" + m_type + ", m_status="
				+ m_status + ", m_content=" + m_content + ", m_approver=" + m_approver + ", m_createTime="
				+ m_createTime + ", m_ownerEmail1=" + m_ownerEmail1 + ", m_ownerEmail2=" + m_ownerEmail2
				+ ", m_lastModifiedTime=" + m_lastModifiedTime + "]";
	};

}
