package com.ctrip.hermes.collector.datasource;

import java.util.ArrayList;
import java.util.List;

import org.unidal.dal.jdbc.AbstractDao;
import org.unidal.dal.jdbc.datasource.DataSourceManager;
import org.unidal.lookup.ContainerLoader;

import com.ctrip.hermes.admin.core.converter.ModelToEntityConverter;
import com.ctrip.hermes.admin.core.model.DatasourceDao;
import com.ctrip.hermes.admin.core.model.DatasourceEntity;
import com.ctrip.hermes.core.utils.PlexusComponentLocator;

public class DbDatasource extends Datasource {
	public static final String MESSAGE = "message";
	private DataSourceManager datasourceManager = PlexusComponentLocator.lookup(DataSourceManager.class);
	private DatasourceDao datasourceDao = PlexusComponentLocator.lookup(DatasourceDao.class);
	public DbDatasource () {
		super(DbDatasourceType.MYSQL);
	}
	
	@Override
	public void resolveDependency() throws Exception {
		List<com.ctrip.hermes.admin.core.model.Datasource> datasources = datasourceDao.list(DatasourceEntity.READSET_FULL);
		List<com.ctrip.hermes.meta.entity.Datasource> messageDatasources = new ArrayList<com.ctrip.hermes.meta.entity.Datasource>();
		for (int index = 0; index < datasources.size(); index++) {
			messageDatasources.add(ModelToEntityConverter.convert(datasources.get(index)));
		}
		ContainerLoader.getDefaultContainer().addComponent(new MessageDataSourceProvider(messageDatasources), MESSAGE); 
	}
	
	public AbstractDao findDao(Class<?> daoClass) {
		return (AbstractDao)PlexusComponentLocator.lookup(daoClass);
	}
	
	public void test() {
		System.out.println(datasourceManager.getDataSource("ds0"));
	}
	
	public enum DbDatasourceType implements DatasourceType {
		MYSQL;
	}
}
