package com.ctrip.hermes.metaservice.service.storage.pojo;

import java.sql.Date;
import java.util.List;

public class StorageTable {

	String name;
	int tableRows;
	// data length in byte.
	long dataLength;

	long indexLength;

	Date createdTime;

	String createOptions;

	public List<StoragePartition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<StoragePartition> partitions) {
		this.partitions = partitions;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getTableRows() {
		return tableRows;
	}

	public void setTableRows(int tableRows) {
		this.tableRows = tableRows;
	}

	public long getDataLength() {
		return dataLength;
	}

	public void setDataLength(long dataLength) {
		this.dataLength = dataLength;
	}

	public long getIndexLength() {
		return indexLength;
	}

	public void setIndexLength(long indexLength) {
		this.indexLength = indexLength;
	}

	public Date getCreatedTime() {
		return createdTime;
	}

	public void setCreatedTime(Date createdTime) {
		this.createdTime = createdTime;
	}

	public String getCreateOptions() {
		return createOptions;
	}

	public void setCreateOptions(String createOptions) {
		this.createOptions = createOptions;
	}

	List<StoragePartition> partitions;

	public StorageTable(String name, int tableRows, long dataLength, long indexLength, Date createdTime, String createOptions) {
		this.name = name;
		this.tableRows = tableRows;
		this.dataLength = dataLength;
		this.indexLength = indexLength;
		this.createdTime = createdTime;
		this.createOptions = createOptions;
	}

	@Override
	public String toString() {
		return "StorageTable{" +
				  "name='" + name + '\'' +
				  ", tableRows=" + tableRows +
				  ", dataLength=" + dataLength +
				  ", indexLength=" + indexLength +
				  ", createdTime=" + createdTime +
				  ", createOptions='" + createOptions + '\'' +
				  ", partitions=" + partitions +
				  '}';
	}
}
