package com.ctrip.hermes.portal.resource;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.core.utils.PlexusComponentLocator;
import com.ctrip.hermes.metaservice.service.TopicService;
import com.ctrip.hermes.metaservice.service.storage.exception.StorageHandleErrorException;
import com.ctrip.hermes.metaservice.service.storage.pojo.StoragePartition;
import com.ctrip.hermes.metaservice.service.storage.pojo.StorageTable;
import com.ctrip.hermes.portal.resource.assists.RestException;

@Path("/storage/")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class TopicStorageResource {

	private TopicService service = PlexusComponentLocator.lookup(TopicService.class);


	//	/storage/size?ds=1&table=5

	/**
	 * Why wrapper primitive value into map?
	 * or Why not just return Integer or String on these REST API?
	 * <p/>
	 * Answer: front-end use ng-resource to request these API, And $resource want them to return object.
	 * $resource can't parse primitive type. (ref to: https://github.com/angular/angular.js/issues/4314)
	 * By Jacob, 6.23.2015.
	 */

	@GET
	@Path("size")
	public Map<String, Object> getSize(@QueryParam("ds") String datasource, @QueryParam("table") String table) {
		checkDatasourceNotNull(datasource);

		Map<String, Object> result = new HashMap<>();
		try {
			if (null == table) {
				result.put("size", service.queryStorageSize(datasource));
			} else {
				result.put("size", service.queryStorageSize(datasource, table));
			}
		} catch (StorageHandleErrorException e) {
			result.put("error", e.getMessage());
		}
		return result;
	}

	@GET
	@Path("tables")
	public List<StorageTable> getTables(@QueryParam("ds") String datasource)
			  throws StorageHandleErrorException {
		checkDatasourceNotNull(datasource);

		// todo: 返回异常到前端的$http.error()中去。
		return service.queryStorageTables(datasource);
	}

	@GET
	@Path("partitions")
	public List<StoragePartition> getTablesPartition(@QueryParam("ds") String datasource,
																	 @QueryParam("table") String table)
			  throws StorageHandleErrorException {
		checkDatasourceNotNull(datasource);

		return service.queryStorageTablePartitions(datasource, table);
	}

	@GET
	@Path("addp")
	public Map<String, String> addPartitionStorage(@QueryParam("ds") String datasource,
														 @QueryParam("table") String table,
														 @QueryParam("span") Integer span) {
		checkDSAndTable(datasource, table);
		Map<String, String> result = new HashMap<>();

		boolean isSuccess = false;
		try {
			service.addPartitionStorage(datasource, table, span);
			isSuccess = true;
		} catch (StorageHandleErrorException e) {
			result.put("error", e.getMessage());
		}
		result.put("result", isSuccess ? "success" : "fail");

		return result;
	}

	@GET
	@Path("deletep")
	public Map<String, String> deletePartitionStorage(@QueryParam("ds") String datasource, @QueryParam("table") String table) {
		checkDSAndTable(datasource, table);

		Map<String, String> result = new HashMap<>();
		boolean isSuccess = false;
		try {
			service.delPartitionStorage(datasource, table);
			isSuccess = true;
		} catch (StorageHandleErrorException e) {
			result.put("error", e.getMessage());
		}
		result.put("result", isSuccess ? "success" : "fail");

		return result;
	}

	private void checkDSAndTable(String datasource, String table) {
		checkDatasourceNotNull(datasource);
		checkTableNotNull(table);
	}

	private void checkDatasourceNotNull(String datasource) {
		if (null == datasource) {
			throw new RestException("datasource is null!");
		}
	}

	private void checkTableNotNull(String table) {
		if (null == table) {
			throw new RestException("table is null!");
		}
	}
}
