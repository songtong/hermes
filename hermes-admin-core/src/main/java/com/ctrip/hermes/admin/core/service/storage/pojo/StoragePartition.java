package com.ctrip.hermes.admin.core.service.storage.pojo;

public class StoragePartition {
	String name;
	String info; // like: Range from..to..
	String range;
	long tableRows;
	long dataLength;
	long indexLength;

	public StoragePartition(String name, String info, String range, Long tableRows, Long dataLength, Long indexLength) {
		this.name = name;
		this.info = info;
		this.range = range;
		this.tableRows = tableRows;
		this.dataLength = dataLength;
		this.indexLength = indexLength;
	}

	public long getTableRows() {
		return tableRows;
	}

	public long getDataLength() {
		return dataLength;
	}

	public long getIndexLength() {
		return indexLength;
	}

	public void setTableRows(long tableRows) {
		this.tableRows = tableRows;
	}

	public void setDataLength(long dataLength) {
		this.dataLength = dataLength;
	}

	public void setIndexLength(long indexLength) {
		this.indexLength = indexLength;
	}

	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}

	public String getInfo() {
		return info;
	}

	public void setInfo(String info) {
		this.info = info;
	}

	public String getRange() {
		return range;
	}

	public void setRange(String range) {
		this.range = range;
	}

	@Override
	public String toString() {
		return "StoragePartition{" +
				  "name='" + name + '\'' +
				  ", info='" + info + '\'' +
				  ", range='" + range + '\'' +
				  ", tableRows=" + tableRows +
				  ", dataLength=" + dataLength +
				  ", indexLength=" + indexLength +
				  '}';
	}
}