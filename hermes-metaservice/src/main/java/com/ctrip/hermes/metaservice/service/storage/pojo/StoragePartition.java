package com.ctrip.hermes.metaservice.service.storage.pojo;

public class StoragePartition {
	String name;
	String info; // like: Range from..to..
	String range;
	Integer tableRows;
	Integer dataLength;
	Integer indexLength;

	public Integer getDataLength() {
		return dataLength;
	}

	public void setDataLength(Integer dataLength) {
		this.dataLength = dataLength;
	}

	public Integer getIndexLength() {
		return indexLength;
	}

	public void setIndexLength(Integer indexLength) {
		this.indexLength = indexLength;
	}

	public StoragePartition(String name, String info, String range, Integer tableRows, Integer dataLength, Integer
			  indexLength) {
		this.info = info;
		this.name = name;
		this.range = range;
		this.tableRows = tableRows;
		this.dataLength = dataLength;
		this.indexLength = indexLength;
	}


	public String getName() {
		return name;
	}

	public Integer getTableRows() {
		return tableRows;
	}

	public void setTableRows(Integer tableRows) {
		this.tableRows = tableRows;
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