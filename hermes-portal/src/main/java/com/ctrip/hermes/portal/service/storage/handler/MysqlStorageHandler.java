package com.ctrip.hermes.portal.service.storage.handler;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.portal.pojo.storage.StoragePartition;
import com.ctrip.hermes.portal.pojo.storage.StorageTable;
import com.ctrip.hermes.portal.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.model.TableModel;

@Named(type = StorageHandler.class, value = MysqlStorageHandler.ID)
public class MysqlStorageHandler implements StorageHandler {

	private static final Logger log = LoggerFactory.getLogger(MysqlStorageHandler.class);

	public static final String ID = "mysql-storage-handler";

	private Connection getMysqlConnection(String jdbcUrl, String usr, String pwd) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(jdbcUrl, usr, pwd);
		} catch (ClassNotFoundException | SQLException e) {
			log.error("Fail to get MySql connection. jdbc:{}, usr:{}", jdbcUrl, usr);
		}
		return conn;
	}

	@Override
	public boolean dropTables(Long topicId, Integer partitionId, List<TableModel> models, String url, String user,
									  String pwd) throws StorageHandleErrorException {
		String databaseName = getDabaseName(url);
		StringBuilder sb = new StringBuilder();
		sb.append(sqlUseDatabase(databaseName));

		String tablePrefix = getTablePrefix(topicId, partitionId);
		for (TableModel model : models) {
			sb.append(sqlDropTable(tablePrefix, model.getTableName()));
		}

		log.warn("URL: " + url + " as " + user + "\nDrop Table Sql: \n" + sb.toString());
		executeSql(url, user, pwd, sb.toString());
		return true;
	}

	@Override
	public void createTable(Long topicId, Integer partitionId, List<TableModel> models, String url, String user,
									String pwd) throws StorageHandleErrorException {
		String databaseName = getDabaseName(url);

		StringBuilder sb = new StringBuilder();
		sb.append(sqlUseDatabase(databaseName));

		topicId = topicId == null ? -1L : topicId;
		partitionId = partitionId == null ? -1 : partitionId;

		String tablePrefix = getTablePrefix(topicId, partitionId);
		for (TableModel model : models) {
			sb.append(sqlCreateTable(tablePrefix, model.getTableName(), model, model.getPks(), model.getIndexMap()));
		}

		log.debug("Build Table Sql: \n" + sb.toString());
		executeSql(url, user, pwd, sb.toString());
	}

	/**
	 * <topicID>_<topic_partitionId>_<table_name> like: 100_0_dead_letter, 900777_0_message_0, 900777_0_resend_1...
	 */
	private String getTablePrefix(Long topicId, Integer partitionId) {
		return topicId + "_" + partitionId + "_";
	}

	/**
	 * databasename分别会是fxhermesshard01db,fxhermesshard02db,fxhermesshard03db
	 */
	private String sqlUseDatabase(String databasename) {
		StringBuilder sb = new StringBuilder();
		sb.append("USE ");
		sb.append(databasename);
		sb.append(" ;\n");

		return sb.toString();
	}

	@Override
	public boolean cleanTable() throws StorageHandleErrorException {
		return false;
	}

	@Override
	public boolean validateDataModel() throws DataModelNotMatchException {
		return false;
	}

	/**
	 * 初始状态是1个100W的partition和1个maxValue的partition和1个minValue的partition. 后续状态是往后新增一个partition.
	 */
	@Override
	public void addPartition(Long topicId, Integer partitionId, TableModel model, int newSapn, String url, String user,
									 String pwd) throws StorageHandleErrorException {
		String databaseName = getDabaseName(url);
		String tableName = getTablePrefix(topicId, partitionId) + model.getTableName();
		List<StoragePartition> storagePartitions = queryPartitionDESC(databaseName, tableName, url, user, pwd);

		StringBuilder sb = new StringBuilder();

		// if there is no Partitions or only Partition pMax left.
		if (storagePartitions.size() == 0
				  || (storagePartitions.size() == 1 && storagePartitions.get(0).getName().equals("pMax"))) {
			sb.append(sqlInitPartition(tableName, "p0", newSapn));
		} else {
			Pair<String /*partitionName*/, Integer /*range*/> highestPartition = getNextHigherPartition
					  (storagePartitions);

			sb.append(sqlAddPartition(tableName, highestPartition.getKey(), highestPartition.getValue() + newSapn));
		}

		executeSql(url, user, pwd, sb.toString());
	}

	private Pair<String /*partitionName*/, Integer /*range*/> getNextHigherPartition
			  (List<StoragePartition> storagePartitions) throws StorageHandleErrorException {
		TreeMap<Integer, StoragePartition> ids = buildPartitionTreeMap(storagePartitions);

		Integer higherId = ids.lastKey() + 1;
		StoragePartition lastPartition = ids.lastEntry().getValue();

		String partitionName = lastPartition.getName().charAt(0) + String.valueOf(higherId);
		int threshold = Integer.parseInt(lastPartition.getRange());
		return new Pair<>(partitionName, threshold);
	}

	/**
	 * 初始化时额外建1个partitin: pMax (MAXVALUE). ALTER TABLE %{tableName} PARTITION BY RANGE (id)( PARTITION %{partitionName} VALUES LESS
	 * THAN %{range} );
	 */
	private String sqlInitPartition(String tableName, String partitionName, int threshold) {
		return "ALTER TABLE " + tableName + " PARTITION BY RANGE (id) (\n" + " PARTITION " + partitionName
				  + " VALUES LESS THAN (" + threshold + ") ENGINE = innodb,"
				  + " PARTITION pMax VALUES LESS THAN MAXVALUE ENGINE = innodb);";
	}

	/**
	 *
	 */
	private String sqlAddPartition(String tableName, String partitionName, int threshold) {
		return "ALTER TABLE " + tableName + " REORGANIZE PARTITION pMax INTO " + "( PARTITION " + partitionName
				  + " VALUES LESS THAN (" + threshold + ") ENGINE = innodb,"
				  + "PARTITION pMax VALUES LESS THAN MAXVALUE ENGINE = innodb);";
	}

	/**
	 * 删除一个最小的partition
	 */
	@Override
	public void deletePartition(Long topicId, Integer partitionId, TableModel model, String url, String user, String pwd)
			  throws StorageHandleErrorException {
		String databaseName = getDabaseName(url);
		String tableName = getTablePrefix(topicId, partitionId) + model.getTableName();
		List<StoragePartition> storagePartitions = queryPartitionDESC(databaseName, tableName, url, user, pwd);

		StringBuilder sb = new StringBuilder();
		sb.append(sqlUseDatabase(databaseName));

		if (storagePartitions.size() == 0) {
			throw new StorageHandleErrorException("No Partition Existed in Table: " + tableName);
		} else {
			if (storagePartitions.size() >= 2) {
				StoragePartition lowestPartition = filterLowestPartition(storagePartitions);

				sb.append(sqlDeletePartition(tableName, lowestPartition.getName()));
			}
		}
		log.warn("Drop Partition Sql: \n" + sb.toString());

		executeSql(url, user, pwd, sb.toString());
	}

	private StoragePartition filterLowestPartition(List<StoragePartition> storagePartitions) {

		TreeMap<Integer, StoragePartition> ids = buildPartitionTreeMap(storagePartitions);

		return ids.firstEntry().getValue();
	}

	private TreeMap<Integer, StoragePartition> buildPartitionTreeMap(List<StoragePartition> storagePartitions) {
		TreeMap<Integer /*parittion id*/, StoragePartition> ids = new TreeMap<>();

		for (StoragePartition storagePartition : storagePartitions) {
			try {
				Integer id = (Integer.parseInt(storagePartition.getName().substring(1)));
				ids.put(id, storagePartition);
			} catch (NumberFormatException e) {
				// ignore "pMax" or other not Integer id.
			}
		}
		return ids;
	}

	@Override
	public List<StorageTable> queryTable(Long topicId, Integer partitionId, String url, String user, String pwd)
			  throws StorageHandleErrorException {

		String databaseName = getDabaseName(url);

		List<StorageTable> tables = queryTables(databaseName, getTablePrefix(topicId, partitionId), url, user, pwd);

		if (tables.size() > 0) {
			for (StorageTable table : tables) {
				List<StoragePartition> partition = queryPartitionDESC(databaseName, table.getName(), url, user, pwd);
				table.setPartitions(partition);
			}
		}
		return tables;
	}

	private String sqlDeletePartition(String tableName, String partitionName) {
		return "ALTER TABLE " + tableName + " DROP PARTITION " + partitionName + ";";
	}

	private String sqlCreateTable(String tableNamePrefix, String tableName, TableModel tableModel,
											TableModel.MetaModel[] pks, Map<String /* indexName */, String[] /* index key */> indexMap) {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE TABLE `").append(tableNamePrefix).append(tableName).append("` (\n");

		sb.append(buildSql(tableModel.getMetaModels()));

		// build: PRIMARY KEY (`id`),
		int tempPKCount = 1;
		sb.append("\nPRIMARY KEY (");
		for (TableModel.MetaModel pk : pks) {
			if (tempPKCount > 1) {
				sb.append(", ");
			}
			sb.append("`");
			sb.append(pk.columnName);
			sb.append("`");

			tempPKCount++;
		}
		sb.append(")");

		// build: INDEX `key` (`ref_key`)
		for (Map.Entry<String, String[]> index : indexMap.entrySet()) {
			String indexKey = index.getKey();
			String[] indexValue = index.getValue();

			int tempIndexValueCount = 1;

			sb.append("\n,INDEX ");
			sb.append("`");
			sb.append(indexKey);
			sb.append("`");
			sb.append("(");

			for (String value : indexValue) {
				if (tempIndexValueCount > 1) {
					sb.append(", ");
				}
				sb.append("`");
				sb.append(value);
				sb.append("`");

				tempIndexValueCount++;
			}
			sb.append(")");
		}

		// match "CREATE TABLE ("
		sb.append("\n)");
		sb.append("ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;\n\n");

		return sb.toString();
	}

	/**
	 * drop table tablename;
	 */
	private String sqlDropTable(String tablePrefix, String tableName) {
		return "DROP TABLE " + tablePrefix + tableName + ";";
	}

	private void executeSql(String jdbcUrl, String usr, String pwd, String sql) throws StorageHandleErrorException {
		Connection conn = getMysqlConnection(jdbcUrl, usr, pwd);
		Statement stmt = null;
		if (null == conn) {
			throw new StorageHandleErrorException("Fail To Get MySql Connection");
		} else {
			try {
				stmt = conn.createStatement();

				String[] tables = sql.split(";");

				for (String table : tables) {
					if (table != null && table.trim().length() > 0) {
						stmt.execute(table.trim() + ";");
					}
				}
			} catch (SQLException e) {
				throw new StorageHandleErrorException(e);
			} finally {
				try {
					if (stmt != null) {
						stmt.close();
					}
					conn.close();
				} catch (Exception e) {
					// ignore it
				}
			}
		}
	}

	private List<StorageTable> queryTables(String databaseName, String tableNamePrefix, String jdbcUrl, String usr,
														String pwd) throws StorageHandleErrorException {
		List<StorageTable> storageTables = new ArrayList<>();
		Connection conn = getMysqlConnection(jdbcUrl, usr, pwd);
		Statement stmt = null;
		if (null == conn) {
			throw new StorageHandleErrorException("Fail To Get MySql Connection");
		} else {
			try {
				stmt = conn.createStatement();

				String queryPartition = "SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, CREATE_TIME, CREATE_OPTIONS "
						  + " FROM information_schema.`TABLES`\n"
						  + " WHERE TABLE_NAME LIKE '"
						  + tableNamePrefix
						  + "%' AND TABLE_SCHEMA = '" + databaseName + "';";

				ResultSet rs = stmt.executeQuery(queryPartition);

				while (rs.next()) {
					storageTables.add(new StorageTable(rs.getString(1), rs.getBigDecimal(2).intValue(), rs.getBigDecimal(3)
							  .intValue(), rs.getBigDecimal(4).intValue(), rs.getDate(5), rs.getString(6)));
				}
				rs.close();

			} catch (SQLException e) {
				throw new StorageHandleErrorException(e);
			} finally {
				try {
					if (stmt != null) {
						stmt.close();
					}
					conn.close();
				} catch (Exception e) {
					// ignore it
				}
			}
		}
		return storageTables;
	}

	private List<StoragePartition> queryPartitionDESC(String databaseName, String tableName, String jdbcUrl, String usr,
																	  String pwd) throws StorageHandleErrorException {
		List<StoragePartition> storagePartitions = new ArrayList<>();
		Connection conn = getMysqlConnection(jdbcUrl, usr, pwd);
		Statement stmt = null;
		if (null == conn) {
			throw new StorageHandleErrorException("Fail To Get MySql Connection");
		} else {
			try {
				stmt = conn.createStatement();

				String queryPartition = "SELECT PARTITION_NAME, PARTITION_METHOD, PARTITION_DESCRIPTION, "
						  + "TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS\nWHERE TABLE_NAME = '" + tableName
						  + "' AND TABLE_SCHEMA = '" + databaseName + "' order by PARTITION_NAME desc";

				ResultSet rs = stmt.executeQuery(queryPartition);

				while (rs.next()) {
					String partitionName = rs.getString(1);
					if (null == partitionName) {
						continue;
					} else {
						storagePartitions.add(new StoragePartition(rs.getString(1), rs.getString(2), rs.getString(3), rs
								  .getBigDecimal(4).intValue()));
					}
				}
				rs.close();

			} catch (SQLException e) {
				throw new StorageHandleErrorException(e);
			} finally {
				try {
					if (stmt != null) {
						stmt.close();
					}
					conn.close();
				} catch (Exception e) {
					// ignore it
				}
			}
		}
		return storagePartitions;
	}

	private String buildSql(TableModel.MetaModel[] metaModels) {

		StringBuilder sb = new StringBuilder();

		for (TableModel.MetaModel metaModel : metaModels) {
			sb.append("`");
			sb.append(metaModel.columnName);
			sb.append("`");
			sb.append(" ");
			sb.append(metaModel.type);
			sb.append(" ");
			sb.append(metaModel.moreInfo);
			sb.append(" ");

			sb.append(", ");
			sb.append("\n");
		}
		return sb.toString();
	}

	private String getDabaseName(String jdbcUrl) {
		String[] strings = jdbcUrl.split("/");
		return strings[strings.length - 1];
	}
}
