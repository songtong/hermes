package com.ctrip.hermes.collector.state;

import org.codehaus.jackson.annotate.JsonIgnore;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.mail.assist.HermesMailDescription.ContentField;
import com.ctrip.hermes.metaservice.service.notify.HermesNotice;
import com.ctrip.hermes.metaservice.service.notify.MailNoticeContent;
import com.ctrip.hermes.metaservice.service.notify.NotifyService;

public abstract class NotifiedState extends State {
	private HermesNotice m_notice;
	public NotifiedState(Object id) {
		super(id);
	}
	
	@JsonIgnore
	public HermesNotice getNotice() {
		if(m_notice==null){
			genarateNotice();
		}
		return m_notice;
	}

	public void setNotice(HermesNotice notice) {
		m_notice = notice;
	}

	public boolean notifyState() {
		return PlexusComponentLocator.lookup(NotifyService.class).notify(m_notice);
	}
	
	protected abstract void genarateNotice(); 

	
}
