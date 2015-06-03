package com.ctrip.hermes.portal.pojo.storage;

public class StoragePartition {
	String info; // like: Range from..to..
	String name;
	Integer range;
	Integer tableRows;

	public StoragePartition(String name, String info, Integer range, Integer tableRows) {
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

	public Integer getRange() {
		return range;
	}

	public void setRange(Integer range) {
		this.range = range;
	}
}