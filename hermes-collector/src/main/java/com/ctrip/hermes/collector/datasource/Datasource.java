package com.ctrip.hermes.collector.datasource;


public abstract class Datasource {
	private DatasourceType m_type;
	private String m_name;
	private boolean m_isDefault;
	
	public Datasource(DatasourceType type) {
		this.m_type = type;
	}
	
	public DatasourceType getType() {
		return m_type;
	}

	public void setType(DatasourceType type) {
		this.m_type = type;
	}

	public String getName() {
		return m_name;
	}

	public void setName(String name) {
		this.m_name = name;
	}
	
	public boolean isDefault() {
		return m_isDefault;
	}

	public void setDefault(boolean isDefault) {
		this.m_isDefault = isDefault;
	}

	public abstract void resolveDependency() throws Exception;

	public interface DatasourceType {};
}
