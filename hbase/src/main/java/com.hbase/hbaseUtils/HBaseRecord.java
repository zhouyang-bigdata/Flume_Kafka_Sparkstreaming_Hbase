package com.hbase.hbaseUtils;

/*
 * @Author zhouyang
 * @Description TODO
 * @Date 10:03 2019/3/5
 * @Param
 * @return
 **/
public class HBaseRecord implements IRecord {
	
	/**
	 * @author libangqin
	 * @copyRight kxnf
	 * @date 2017-09-12
	 * @detail 描述hbase查询的一条记录的一个列的对应键值对信息
	 * */
	
	public HBaseRecord() {
	}
	private String rowKey; //rowkey
	private String family; //列族名称
	private String qualifier; //列名
	private String value;
	private long timestamp;
	private boolean isIncr;

	/**
	 * 构造函数
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 */
	public HBaseRecord(String rowKey, String family, String qualifier, String value) {
		this(rowKey, family, qualifier, value, DateUtil.getCurrentMsTime());
	}
	
	public HBaseRecord(String family, String qualifier, String value) {
		this(family, qualifier, value, DateUtil.getCurrentMsTime(),false);
	}

	/**
	 * 构造函数
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @param timestamp
	 */
	public HBaseRecord(String rowKey, String family, String qualifier, String value, long timestamp) {
		this(rowKey, family, qualifier, value, timestamp, false);
	}

	/**
	 * 构造函数
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @param timestamp
	 * @param isIncr
	 */
	public HBaseRecord(String rowKey, String family, String qualifier, String value, long timestamp, boolean isIncr) {
		this.rowKey = StringUtil.isNullOrEmpty(rowKey) ? "" : rowKey.toUpperCase();
		this.family = family;
		this.qualifier = qualifier;
		this.value = value;
		this.isIncr = isIncr;
		this.timestamp = timestamp;
	}
	
	public HBaseRecord(String family, String qualifier, String value, long timestamp, boolean isIncr) {
		this.rowKey = StringUtil.isNullOrEmpty(rowKey) ? "" : rowKey.toUpperCase();
		this.family = family;
		this.qualifier = qualifier;
		this.value = value;
		this.isIncr = isIncr;
		this.timestamp = timestamp;
	}

	public String getRowKey() {
		return rowKey;
	}

	public String getFamily() {
		return family;
	}

	public String getQualifier() {
		return qualifier;
	}

	public String getValue() {
		return value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	/**
	 * @return the isIncr
	 */
	public boolean isIncr() {
		return isIncr;
	}

	/**
	 * @param isIncr
	 *            the isIncr to set
	 */
	public void setIncr(boolean isIncr) {
		this.isIncr = isIncr;
	}

	@Override
	public String getString() {
		return String.format("rowKey:%s, family:%s,qualifier:%s,timestamp:%s,value:%s", this.rowKey, this.family,
				this.qualifier, this.timestamp, this.value);
	}
}
