package com.ctrip.hermes.metaservice.service.storage.pojo;

public class StoragePartition {
	String info; // like: Range from..to..
	String name;
	String range;
	Integer tableRows;

	public StoragePartition(String name, String info, String range, Integer tableRows) {
		this.info = info;
		this.name = name;
		this.range = range;
		this.tableRows = tableRows;
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
				  "info='" + info + '\'' +
				  ", name='" + name + '\'' +
				  ", range='" + range + '\'' +
				  ", tableRows=" + tableRows +
				  '}';
	}
}