package com.ctrip.hermes.rest.service.storage.handler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.rest.service.storage.exception.DataModelNotMatchException;
import com.ctrip.hermes.rest.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.rest.service.storage.model.TableModel;


@Named(type = StorageHandler.class)
public class MysqlStorageHandler implements StorageHandler {

	private static final Logger log = LoggerFactory.getLogger(MysqlStorageHandler.class);
	@Inject
	private MetaService metaService;

	@Override
	public boolean dropTables() throws StorageHandleErrorException {
		return false;
	}

	@Override
	public void createTable(String databaseName, Long topicId, Integer partitionId, List<TableModel> models)
			  throws StorageHandleErrorException {
		StringBuilder sb = new StringBuilder();
		sb.append(sqlUseDatabase(databaseName));

		topicId = topicId == null ? -1L : topicId;
		partitionId = partitionId == null ? -1 : partitionId;

		String tablePrefix = topicId + "_" + partitionId + "_";
		for (TableModel model : models) {
			sb.append(sqlCreateTable(tablePrefix, model.getTableName(), model, model.getPks(), model
					  .getIndexMap()));
		}

		// todo: log
		log.debug("Build Table Sql: \n" + sb.toString());
		executeSql(sb.toString());
	}

	private void executeSql(String sql) throws StorageHandleErrorException {

		final String JDBC_URL = "jdbc:mysql://10.3.6.237:3306";
		final String MYSQL_USER = "root";
		final String MYSQL_USER_PSW = "xxxxx";

		Connection conn = null;
		Statement stmt = null;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(JDBC_URL, MYSQL_USER, MYSQL_USER_PSW);

			stmt = conn.createStatement();

			String[] tables = sql.split(";");

			for (String table : tables) {
				if (table != null && table.trim().length() > 0) {
					stmt.execute(table.trim() + ";");
				}
			}

		} catch (ClassNotFoundException | SQLException e) {
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
	}

	/**
	 * databasename分别会是fxhermesshard01db,fxhermesshard02db,fxhermesshard03db
	 */
	public String sqlUseDatabase(String databasename) {
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

	public String sqlCreateTable(String tableNamePrefix, String tableName, TableModel tableModel, TableModel.MetaModel[]
			  pks, Map<String /*indexName*/, String[] /*index key*/> indexMap) {
		StringBuilder sb = new StringBuilder();

		sb.append("CREATE TABLE `")
				  .append(tableNamePrefix)
				  .append(tableName)
				  .append("` (\n");


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


		// build: 	INDEX `key` (`ref_key`)
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
}
