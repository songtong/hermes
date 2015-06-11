package com.ctrip.hermes.metaserver.meta;

/**
 * 
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class MetaVersionInfo {
	private long m_version;

	private String m_owner;

	public MetaVersionInfo(long version, String owner) {
		m_version = version;
		m_owner = owner;
	}

	public long getVersion() {
		return m_version;
	}

	public String getOwner() {
		return m_owner;
	}

}