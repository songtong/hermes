package com.ctrip.hermes.portal.service.storage.model;

import java.util.HashMap;
import java.util.Map;

public class TableModel {
	MetaModel[] pks;
	Map<String /*indexName*/, String[] /*index key*/> indexMap = new HashMap<>();
	MetaModel[] metaModels;
	String tableName = null;

	public MetaModel[] getPks() {
		return pks;
	}

	public Map<String, String[]> getIndexMap() {
		return indexMap;
	}

	public MetaModel[] getMetaModels() {
		return metaModels;
	}

	/**
	 * overload by subclass
	 *
	 * @return table name
	 */
	public String getTableName() {
		if (tableName == null) {
			throw new RuntimeException("TableName not Set!");
		}
		return tableName;
	}


	//TODO: 这个MetaModel是每个表自己分别建，但实际上有相同的设置(例:'id')，将来可抽成枚举.
	public class MetaModel {
		public String columnName;
		public String type;
		public String moreInfo;


		public MetaModel(String columnName, String type, String moreInfo) {
			this.columnName = columnName;
			this.type = type;
			this.moreInfo = moreInfo;
		}
	}

	/**
	 * build:
	 * `id` BIGINT(11) UNSIGNED NOT NULL AUTO_INCREMENT
	 */
	protected void setMetaModels(MetaModel... metaModels) {
		this.metaModels = metaModels;
	}

	/**
	 * build:
	 * PRIMARY KEY (`id`)
	 */
	protected void setPrimaryKey(MetaModel... pks) {
		this.pks = pks;
	}

	/**
	 * build:
	 * INDEX `key` (`ref_key`)
	 */
	protected void setIndexKey(String indexName, String... indexKeys) {
		indexMap.put(indexName, indexKeys);
	}

	protected void setTableName(String tableName) {
		this.tableName = tableName;
	}
}
