package com.ctrip.hermes.collector.datasource;

public class HttpDatasource extends Datasource {
	private String m_api;
	
	public HttpDatasource(DatasourceType type) {
		super(type);
	}

	public String getApi() {
		return m_api;
	}

	public void setApi(String api) {
		this.m_api = api;
	}

	@Override
	public void resolveDependency() {		
	}
	
	public enum HttpDatasourceType implements DatasourceType {
		CAT, ES;
	}

}
