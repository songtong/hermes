package com.ctrip.hermes.portal.service.storage.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.portal.pojo.storage.StoragePartition;
import com.ctrip.hermes.portal.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.portal.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.portal.service.storage.model.TableModel;

@Named(type = StorageHandler.class)
public class MysqlStorageHandler implements StorageHandler {

	private static final Logger log = LoggerFactory.getLogger(MysqlStorageHandler.class);


	private Connection getMysqlConnection(String jdbcUrl, String usr, String pwd) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(jdbcUrl, usr, pwd);
		} catch (ClassNotFoundException | SQLException e) {
			// todo: log
			e.printStackTrace();
		}
		return conn;
	}

	@Override
	public boolean dropTables(Long topicId, Integer partitionId, List<TableModel> models,
									  String url, String user, String pwd) throws StorageHandleErrorException {
		String databaseName = getDabaseName(url);
		StringBuilder sb = new StringBuilder();
		sb.append(sqlUseDatabase(databaseName));

		String tablePrefix = getTablePrefix(topicId, partitionId);
		for (TableModel model : models) {
			sb.append(sqlDropTable(tablePrefix, model.getTableName()));
		}

		log.warn("Drop Table Sql: \n" + sb.toString());
		executeSql(url, user, pwd, sb.toString());
		return true;
	}

	@Override
	public void createTable(Long topicId, Integer partitionId, List<TableModel> models,
									String url, String user, String pwd)
	      throws StorageHandleErrorException {
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
	 * <topicID>_<topic_partitionId>_<table_name>
	 * like: 100_0_dead_letter, 900777_0_message_0, 900777_0_resend_1...
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
	 * 初始状态是1个100W的partition和1个maxValue的partition和1个minValue的partition.
	 * 后续状态是往后新增一个partition.
	 */
	@Override
	public void addPartition(Long topicId, Integer partitionId, TableModel model, int range,
									 String url, String user, String pwd)
			  throws StorageHandleErrorException {
		String databaseName = getDabaseName(url);
		String tableName = getTablePrefix(topicId, partitionId) + model.getTableName();
		List<StoragePartition> storagePartitions = queryPartitionDESC(databaseName, tableName, url, user, pwd);


		StringBuilder sb = new StringBuilder();
		sb.append(sqlUseDatabase(databaseName));

		if (storagePartitions.size() == 0) {
			sb.append(sqlInitPartition(tableName, "p0", range));
		} else {
			StoragePartition higherPartition = storagePartitions.get(storagePartitions.size());
			String partitionName;
			try {
				partitionName = higherPartition.getName().charAt(0) +
						  String.valueOf(Integer.parseInt(higherPartition.getName().substring(1)) + 1);
			} catch (NumberFormatException e) {
				throw new StorageHandleErrorException("Fail to Parse Partition Name: " + higherPartition.getName());
			}

			int newRange = higherPartition.getRange() + range;
			sb.append(sqlAddPartition(tableName, partitionName, newRange));
		}

		executeSql(url, user, pwd, sb.toString());
	}

	/**
	 * ALTER TABLE %{tableName} PARTITION BY RANGE (id)(
	 * PARTITION %{partitionName} VALUES LESS THAN %{range} );
	 */
	private String sqlInitPartition(String tableName, String partitionName, int range) {
		return "ALTER TABLE " + tableName + "PARTITION BY RANGE (id) (\n"
				  + "PARTITION " + partitionName + "VALUES LESS " + range + " );";
	}

	/**
	 * ALTER TABLE %{tableName} ADD PARTITION (PARTITION %{partitionName} VALUES LESS THAN (%{range}));
	 */
	private String sqlAddPartition(String tableName, String partitionName, int range) {
		return "ALTER TABLE" + tableName + " ADD PARTITION (PARTITION " + partitionName
				  + " VALUES LESS THAN (" + range + "));";
	}

	/**
	 * 删除一个最小的partition
	 */
	@Override
	public void deletePartition(Long topicId, Integer partitionId, TableModel model,
										 String url, String user, String pwd)
			  throws StorageHandleErrorException {
		String databaseName = getDabaseName(url);
		String tableName = getTablePrefix(topicId, partitionId) + model.getTableName();
		List<StoragePartition> storagePartitions = queryPartitionDESC(databaseName, tableName, url, user, pwd);

		StringBuilder sb = new StringBuilder();
		sb.append(sqlUseDatabase(databaseName));

		if (storagePartitions.size() == 0) {
			throw new StorageHandleErrorException("No Partition Existed in Table: " + tableName);
		} else {
			if (storagePartitions.size() > 2) {
				StoragePartition lowestPartition = storagePartitions.get(storagePartitions.size());

				sb.append(sqlDeletePartition(tableName, lowestPartition.getName()));
			}
		}
		log.warn("Drop Partition Sql: \n" + sb.toString());

		executeSql(url, user, pwd, sb.toString());
	}

	private String sqlDeletePartition(String tableName, String partitionName) {
		return "ALTER TABLE " + tableName + " DROP " + partitionName + ";";
	}

	private String sqlCreateTable(String tableNamePrefix, String tableName, TableModel tableModel, TableModel.MetaModel[]
			  pks, Map<String /*indexName*/, String[] /*index key*/> indexMap) {
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

	private List<StoragePartition> queryPartitionDESC(String databaseName, String tableName, String jdbcUrl, String
			  usr, String pwd)
			  throws StorageHandleErrorException {
		List<StoragePartition> storagePartitions = new ArrayList<>();
		Connection conn = getMysqlConnection(jdbcUrl, usr, pwd);
		Statement stmt = null;
		if (null == conn) {
			throw new StorageHandleErrorException("Fail To Get MySql Connection");
		} else {
			try {
				stmt = conn.createStatement();

				String queryPartition =  /*sqlUseDatabase(databaseName)*/
						  "SELECT PARTITION_NAME, PARTITION_METHOD, PARTITION_DESCRIPTION, "
						  + "TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS\n"
						  + "WHERE TABLE_NAME = '" + tableName +"' order by PARTITION_NAME desc";

				ResultSet rs = stmt.executeQuery(queryPartition);

				while(rs.next()){
					String partitionName = rs.getString(0);
					if (null == partitionName) {
						continue;
					} else {
						storagePartitions.add(new StoragePartition(rs.getString(0),
								  rs.getString(1), Integer.parseInt(rs.getString(2)), rs.getBigDecimal(3).intValue()));
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
