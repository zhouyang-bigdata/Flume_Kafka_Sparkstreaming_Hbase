package com.hbase.example;


import com.hbase.example.client.HBaseConnPool;
import com.hbase.example.client.HBaseConnPoolManager;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @Author zhouyang
 * @Description TODO
 * @Date 15:51 2019/2/25
 * @Param
 * @return
 **/
public class HBaseBaseHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseBaseHandler.class);

	private String encoding = "UTF-8";

	private HBaseConnPool connPool;

	public HBaseBaseHandler(HBaseConnPool connPool) {
		this.connPool = connPool;
	}

	public HBaseBaseHandler() {
		this.connPool = new HBaseConnPoolManager();
	}

	public HBaseConnPool getConnPool() {
		return connPool;
	}

	public HTableInterface getTable(String tableName) throws Exception {
		return connPool.getConn().getTable(tableName.getBytes(encoding));
	}

	public Boolean tableExist(String tableName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(connPool.getConn());
			return admin.tableExists(tableName);
		} catch (Exception ex) {
			LOGGER.error("exist htable err", ex);
		} finally {
			HBaseUtils.close(admin);
		}
		return false;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}
}
