package com.ctrip.hermes.metaservice.service.storage.handler;

import java.sql.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.metaservice.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.model.TableModel;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;
import com.mchange.v2.c3p0.ComboPooledDataSource;

@Named(type = StorageHandler.class, value = MysqlStorageHandler.ID)
public class MysqlStorageHandler implements StorageHandler {

	private static final Logger log = LoggerFactory.getLogger(MysqlStorageHandler.class);

	public static final String ID = "mysql-storage-handler";

	private ComboPooledDataSource buildConnectionPool(String jdbcUrl, String usr, String pwd) throws StorageHandleErrorException {
		String driver = "com.mysql.jdbc.Driver";
		ComboPooledDataSource cpds = new ComboPooledDataSource();
		try {
			cpds.setDriverClass(driver);
			cpds.setJdbcUrl(jdbcUrl);
			cpds.setUser(usr);
			cpds.setPassword(pwd);
			cpds.setMinPoolSize(1);
			cpds.setMaxPoolSize(3);
			cpds.setInitialPoolSize(1);
			cpds.setMaxIdleTime(10 * 60);
			cpds.setIdleConnectionTestPeriod(60);
			cpds.setAcquireRetryAttempts(1);
			cpds.setAcquireRetryDelay(30);
			cpds.setMaxStatements(0);
			cpds.setMaxStatementsPerConnection(1000);
			cpds.setNumHelperThreads(6);
			cpds.setMaxAdministrativeTaskTime(5);
			cpds.setPreferredTestQuery("SELECT 1");
			cpds.setLoginTimeout(30);

			cpds.getConnection().close();
		} catch (Throwable e) {
			cpds.close();
			throw new StorageHandleErrorException(String.format("Error when connecting to JDBC data source "
					  + "with properties (driver=%s, url=%s, user=%s). Error message=%s", driver, jdbcUrl, usr, e));
		}

		return cpds;
	}

	Map<String, ComboPooledDataSource> connectionPools = new HashMap<>();

	private Connection getMysqlConnection(String jdbcUrl, String usr, String pwd) {
		ComboPooledDataSource cpds = null;
		Connection conn = null;
		String key = jdbcUrl + usr + pwd;
		if (connectionPools.containsKey(key)) {
			cpds = connectionPools.get(key);
		} else {
			try {
				cpds = buildConnectionPool(jdbcUrl, usr, pwd);
			} catch (StorageHandleErrorException e) {
				log.error("Fail to create ConnectionPool to {} as {}.\n{}", jdbcUrl, usr, e);
			}
			connectionPools.put(key, cpds);
			log.info("Created ConnectionPool to {} as {}", jdbcUrl, usr);
		}

		try {
			conn = cpds.getConnection();
		} catch (SQLException e) {
			log.error("Fail to get connection from ConnectionPool to {} as {}.\n{}", jdbcUrl, usr, e);
		}
		return conn;
	}

	@Override
	public boolean dropTables(Long topicId, Integer partitionId, List<TableModel> models, String url, String user,
									  String pwd) throws StorageHandleErrorException {
		String databaseName = getDatabaseName(url);
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
		String databaseName = getDatabaseName(url);

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
		String tableName = getTablePrefix(topicId, partitionId) + model.getTableName();
		addPartition0(tableName, newSapn, url, user, pwd);
	}

	private void addPartition0(String tableName, int newSapn, String url, String user, String pwd) throws StorageHandleErrorException {
		String databaseName = getDatabaseName(url);
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

	@Override
	public void addPartition(String table, int range, String url, String user, String pwd) throws StorageHandleErrorException {
		addPartition0(table, range, url, user, pwd);
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
		String tableName = getTablePrefix(topicId, partitionId) + model.getTableName();
		deletaPartition0(tableName, url, user, pwd);
	}

	private void deletaPartition0(String tableName, String url, String user, String pwd) throws
			  StorageHandleErrorException {
		String databaseName = getDatabaseName(url);
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

	@Override
	public void deletePartition(String table, String url, String user, String pwd) throws StorageHandleErrorException {
		deletaPartition0(table, url, user, pwd);
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

		String databaseName = getDatabaseName(url);

		List<StorageTable> tables = queryTables(databaseName, getTablePrefix(topicId, partitionId), url, user, pwd);

		if (tables.size() > 0) {
			for (StorageTable table : tables) {
				List<StoragePartition> partition = queryPartitionDESC(databaseName, table.getName(), url, user, pwd);
				table.setPartitions(partition);
			}
		}
		return tables;
	}

	@Override
	public List<StorageTable> queryAllTablesInDatasourceWithoutPartition(String url, String user, String pwd) throws StorageHandleErrorException {
		String databaseName = getDatabaseName(url);

		return queryTables(databaseName, "", url, user, pwd);
	}

	@Override
	public Integer queryAllSizeInDatasource(String url, String user, String pwd) throws StorageHandleErrorException {
		String databaseName = getDatabaseName(url);
		return querySize(databaseName, null, url, user, pwd);
	}

	@Override
	public Integer queryTableSize(String table, String url, String user, String pwd) throws StorageHandleErrorException {
		String databaseName = getDatabaseName(url);
		return querySize(databaseName, table, url, user, pwd);
	}

	@Override
	public List<StoragePartition> queryTablePartitions(String table, String url, String user, String pwd) throws StorageHandleErrorException {
		String databaseName = getDatabaseName(url);
		return queryPartitionDESC(databaseName, table, url, user, pwd);
	}

	private String sqlDeletePartition(String tableName, String partitionName) {
		return "ALTER TABLE " + tableName + " DROP PARTITION " + partitionName + ";";
	}

	private String sqlCreateTable(String tableNamePrefix, String tableName, TableModel tableModel,
											TableModel.MetaModel[] pks, Map<String /* indexName */, String[] /* index key */> indexMap) {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE TABLE `").append(tableNamePrefix).append(tableName).append("` (\n");

		sb.append(sqlCreateTable(tableModel.getMetaModels()));

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

	private Integer querySize(String databaseName, String table, String url, String usr, String pwd)
			  throws StorageHandleErrorException {
		Connection conn = getMysqlConnection(url, usr, pwd);
		Statement stmt = null;

		Integer result = null;
		if (null == conn) {
			throw new StorageHandleErrorException("Fail To Get MySql Connection");
		} else {
			try {
				stmt = conn.createStatement();

				StringBuilder sb = new StringBuilder();
				sb.append("SELECT table_schema, SUM(data_length + index_length)\n" +
						  "FROM information_schema.TABLES\n" +
						  "WHERE table_schema = '")
						  .append(databaseName)
						  .append("' ");
				if (null != table) {
					sb.append("AND table_name = '")
							  .append(table)
							  .append("' ");
				}
				sb.append("GROUP BY table_schema");

				ResultSet rs = stmt.executeQuery(sb.toString());

				while (rs.next()) {
					result = rs.getBigDecimal(2).intValue();
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
				} catch (SQLException e) {
					throw new StorageHandleErrorException(e);
				}
			}
		}
		return result;
	}

	private List<StorageTable> queryTables(String databaseName, String tableNamePrefix, String jdbcUrl, String usr,
														String pwd) throws StorageHandleErrorException {
		List<StorageTable> storageTables = new ArrayList<>();
		PreparedStatement stmt = null;

		Connection conn = null;
		try {
			conn = getMysqlConnection(jdbcUrl, usr, pwd);
			stmt = conn.prepareStatement("SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, CREATE_TIME, CREATE_OPTIONS "
					  + " FROM information_schema.`TABLES`\n"
					  + " WHERE TABLE_NAME LIKE ? AND TABLE_SCHEMA = ? ;");

			stmt.setString(1, tableNamePrefix + "%");
			stmt.setString(2, databaseName);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				storageTables.add(new StorageTable(rs.getString(1), rs.getBigDecimal(2).longValue(), rs.getBigDecimal(3)
						  .longValue(), rs.getBigDecimal(4).longValue(), rs.getDate(5), rs.getString(6)));
			}
			rs.close();

		} catch (SQLException e) {
			throw new StorageHandleErrorException(e);
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				// ignore it
			}
		}
		return storageTables;
	}

	private List<StoragePartition> queryPartitionDESC(String databaseName, String tableName, String jdbcUrl, String usr,
																	  String pwd) throws StorageHandleErrorException {
		List<StoragePartition> storagePartitions = new ArrayList<>();
		Connection conn = null;
		PreparedStatement stmt = null;
		try {
			conn = getMysqlConnection(jdbcUrl, usr, pwd);
			stmt = conn.prepareStatement("SELECT PARTITION_NAME, PARTITION_METHOD, PARTITION_DESCRIPTION," +
					  "TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ? ORDER" +
					  " BY PARTITION_NAME DESC");

			stmt.setString(1, tableName);
			stmt.setString(2, databaseName);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				String partitionName = rs.getString(1);
				if (null == partitionName) {
					continue;
				} else {
					storagePartitions.add(new StoragePartition(rs.getString(1), rs.getString(2), rs.getString(3), rs
							  .getBigDecimal(4).longValue(), rs.getBigDecimal(5).longValue(), rs.getBigDecimal(6).longValue()));
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
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				// ignore it
			}
		}
		return storagePartitions;
	}

	private String sqlCreateTable(TableModel.MetaModel[] metaModels) {

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

	private String getDatabaseName(String jdbcUrl) {
		String[] strings = jdbcUrl.split("/");
		return strings[strings.length - 1];
	}
}
