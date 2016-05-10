package com.ctrip.hermes.collector.datasource;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ctrip.hermes.collector.conf.CollectorConfiguration;
import com.ctrip.hermes.collector.datasource.Datasource.DatasourceType;

/**
 * @author tenglinxiao
 *
 */
@Component
public class DatasourceManager {
	private Map<DatasourceType, Map<String, Datasource>> m_datasources = new HashMap<DatasourceType, Map<String, Datasource>>(); 
	@Autowired
	private CollectorConfiguration m_conf;
	
	@PostConstruct
	protected void init() {
		initHttpDatasource();
		initDbDatasource();
	}
	
	private void initHttpDatasource() {
		EsDatasource ds = new EsDatasource();
		ds.setName(m_conf.getEsDatsourceName());
		ds.setApi(m_conf.getEsDatasourceApi());
		ds.setTokenFile(m_conf.getEsTokenFile());
		ds.resolveDependency();
		
		addDatasource(ds);
		
		CatDatasource cds = new CatDatasource();
		cds.setName("Cat");
		cds.setApi(m_conf.getCatBaseApi());

		addDatasource(cds);
	}
	
	private void initDbDatasource() {
		
	}
	
	public void addDatasource(Datasource ds) {
		if (!m_datasources.containsKey(ds.getType())) {
			m_datasources.put(ds.getType(), new HashMap<String, Datasource>());
		}
		m_datasources.get(ds.getType()).put(ds.getName(), ds);
	}
	
	public Datasource getDefaultDatasource(DatasourceType type) {
		Map<String, Datasource> datasources = m_datasources.get(type);
		if (datasources == null || datasources.size() == 0) {
			return null;
		}
		
		for (Datasource datasource : datasources.values()) {
			if (datasource.isDefault()) {
				return datasource;
			}
		}
		return datasources.values().iterator().next();
	}
}
