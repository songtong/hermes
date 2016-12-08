package com.ctrip.hermes.collector.state.impl;

import com.ctrip.hermes.collector.state.State;

public class CommandDropState extends State {
	public static String ID_FORMAT = "%s-%s-%d";
	
    private String m_commandType;
    
    private short m_minute;
    
    private long m_count;
    
    private String m_host;
    
    public CommandDropState(String commandType, short minute, long count, String host) {
        this.m_commandType = commandType;
        this.m_minute = minute;
        this.m_count = count;
        this.m_host = host;
    }
    
    public String getCommandType() {
        return m_commandType;
    }

    public void setCommandType(String commandType) {
        m_commandType = commandType;
    }

    public short getMinute() {
		return m_minute;
	}

	public void setMinute(short minute) {
		m_minute = minute;
	}

	public long getCount() {
        return m_count;
    }

    public void setCount(long count) {
        m_count = count;
    }
    
    public String getHost() {
		return m_host;
	}

	public void setHost(String host) {
		m_host = host;
	}

	@Override
    protected void doUpdate(State state) {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected Object generateId() {
        return String.format(ID_FORMAT, m_commandType, m_host, System.currentTimeMillis());
    }

}
