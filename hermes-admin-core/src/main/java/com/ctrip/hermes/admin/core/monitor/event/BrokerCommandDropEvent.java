package com.ctrip.hermes.admin.core.monitor.event;

import com.ctrip.hermes.admin.core.monitor.MonitorEventType;

public class BrokerCommandDropEvent extends BaseMonitorEvent {
    private String m_command;
    private long m_count;
    private String m_date;
    private String m_host;
    private String m_group;

    public BrokerCommandDropEvent() {
        super(MonitorEventType.BROKER_COMMAND_DROP);
    }
    
    public String getCommand() {
        return m_command;
    }

    public void setCommand(String command) {
        m_command = command;
    }

    public long getCount() {
        return m_count;
    }

    public void setCount(long count) {
        m_count = count;
    }
    
    public String getDate() {
		return m_date;
	}

	public void setDate(String date) {
		m_date = date;
	}
	
	public String getHost() {
		return m_host;
	}

	public void setHost(String host) {
		m_host = host;
	}
	
	public String getGroup() {
		return m_group;
	}

	public void setGroup(String group) {
		m_group = group;
	}

	@Override
    protected void parse0(com.ctrip.hermes.admin.core.model.MonitorEvent dbEntity) {
        m_command = dbEntity.getKey1();
        m_count = Long.parseLong(dbEntity.getKey2());
        m_date = dbEntity.getKey3();
    }

    @Override
    protected void toDBEntity0(com.ctrip.hermes.admin.core.model.MonitorEvent e) {
        e.setKey1(m_command);
        e.setKey2(String.valueOf(m_count));
        e.setKey3(m_date);
        e.setMessage(String.format("*Broker command drop type: %s, count: %d, date: %s", m_command, m_count, m_date));
    }

}
