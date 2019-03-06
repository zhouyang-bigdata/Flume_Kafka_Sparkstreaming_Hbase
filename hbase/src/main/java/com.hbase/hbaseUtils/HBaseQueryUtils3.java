package com.hbase.hbaseUtils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * @ClassName HBaseQueryUtils3
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/3/6 14:31
 * @Version 1.0
 **/
public class HBaseQueryUtils3 {
    public static Logger log = Logger.getLogger(HBaseQueryUtils2.class);
    private Map<String, HTableInterface> HTableInterfaces = new HashMap<String, HTableInterface>();
    private Object lockObj = StringUtil.getEmptyString();
    private Configuration conf = null;
    private HBaseAdmin admin = null;
    private HConnection connection = null;

    private List<String> tableNames;

    private static String configName = "/hbase_elasticsearch.properties";
    private String tableName;
    private String rowKeyStr;
    private String qualifierStr;
    private String valueStr;



    public String getRowKeyStr() {
        return this.rowKeyStr;
    }




    public void setRowKeyStr(String rowKeyStr) {
        this.rowKeyStr = rowKeyStr;
    }




    public String getQualifierStr() {
        return this.qualifierStr;
    }




    public void setQualifierStr(String qualifierStr) {
        this.qualifierStr = qualifierStr;
    }




    public String getValueStr() {
        return this.valueStr;
    }




    public void setValueStr(String valueStr) {
        this.valueStr = valueStr;
    }

    public static void printProperties() throws Exception {
        InputStream in = HBaseQueryUtils3.class.getClassLoader().getResourceAsStream(configName);
        Properties pro = new Properties();
        pro.load(in);
        System.out.println("url: " + pro.getProperty("url")
                + "user: " + pro.getProperty("user")
                + "password: " + pro.getProperty("password"));
    }

    public HBaseQueryUtils3(){
        //PropertiesLoader propertiesLoader = new PropertiesLoader(configName);
        InputStream in = HBaseQueryUtils3.class.getClassLoader().getResourceAsStream(configName);
        Properties propertiesLoader = new Properties();
        try{
            propertiesLoader.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }


        conf = HBaseConfiguration.create();
        // 使用kafka连接hbase的配置
        try {
            conf.set("hbase.zookeeper.quorum",
                    propertiesLoader.getProperty("HBASE_ZOOKEEPER_QUORUM"));
            conf.set("hbase.table.sanity.checks", "true");
            // 提高RPC通信时长
            conf.setLong("hbase.rpc.timeout",
                    Integer.parseInt(propertiesLoader.getProperty("HBASE_RPC_TIMEOUT")));
            // 设置Scan缓存 默认1000条记录
            conf.setLong("hbase.client.scanner.caching", Integer
                    .parseInt(propertiesLoader.getProperty("HBASE_CLIENT_SCANER_CACHING")));
            conf.setLong("zookeeper.session.timeout", Integer
                    .parseInt(propertiesLoader.getProperty("ZOOKEEPER_SESSION_TIMEOUT")));
            tableNames = new ArrayList<String>();

            this.getAdmin();

            //获取表名
            String getTableName = propertiesLoader.getProperty("HBASE_TABLE_NAME");
            this.tableName = getTableName;
            this.rowKeyStr = propertiesLoader.getProperty("hbase.table.rowKeyStr");
            this.qualifierStr = propertiesLoader.getProperty("hbase.table.qualifierStr");
            this.valueStr = propertiesLoader.getProperty("hbase.table.param.valueStr");
        } catch (IOException e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * @Title: getTableName
     * @Description: TODO(获取表名)
     * @param: @return
     * @return: String
     * @throws
     */
    public String getTableName() {
        return this.tableName;
    }

    public void setTableName() throws FileNotFoundException, IOException {
        //PropertiesLoader propertiesLoader = new PropertiesLoader(configName);
        InputStream in = HBaseQueryUtils3.class.getClassLoader().getResourceAsStream(configName);
        Properties propertiesLoader = new Properties();
        propertiesLoader.load(in);

        String getTableName = propertiesLoader.getProperty("HBASE_TABLE_NAME");
        this.tableName = getTableName;
    }

    public HTableInterface getTable(String tableName) {
        try {
            if (StringUtil.isNullOrEmpty(tableName))
                throw new Exception("add data to the Hadoop, tableName does not allow for Null Or Empty");
            if (!this.HTableInterfaces.containsKey(tableName)) {
                synchronized (this.lockObj) {
                    if (!this.HTableInterfaces.containsKey(tableName)) {
                        HConnection connection = this.getConnection();
                        HTableInterface table = connection.getTable(Bytes.toBytes(tableName));
                        this.HTableInterfaces.put(tableName, table);
                    }
                }
            }
            return this.HTableInterfaces.get(tableName);
        } catch (Exception ex) {
            log.error(ex.getMessage());
            ex.printStackTrace();
        }
        return null;
    }

    private synchronized HConnection getConnection() throws IOException {
        if (this.connection == null) {
            this.connection = HConnectionManager.createConnection(conf);
        }
        return connection;
    }

    public synchronized HBaseAdmin getAdmin()
            throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        if (admin == null) {
            admin = new HBaseAdmin(this.getConnection().getConfiguration());
        }
        return admin;
    }


    /**
     * @author Administrator
     * @detail 简单的get查询
     * @date 2018-01-08
     * @param rowRey
     *            请求Bean实体
     *
     */

    public Map<String, Map<String, List<HBaseRecord>>> singleGetQuery(String rowRey) {
        String mytableName = getTableName();
        Map<String, Map<String, List<HBaseRecord>>> tableRecordsMap = new HashMap<String, Map<String, List<HBaseRecord>>>();
        if (StringUtil.isNullOrEmpty(mytableName)) {
            log.error("tableName cann't be empty please check");
            System.out.println("tableName cann't be empty please check");
            return tableRecordsMap;
        }
        Get get = singleGetCondition(rowRey);
        List<Get> gets = new ArrayList<Get>();
        if (get != null) {
            gets.add(get);
        } else {
            log.error("there not any condition match request params");
        }
        return this.batchGetRecords(mytableName, gets);
    }


    /**
     * @author Administrator
     * @detail 简单的get查询
     * @date 2018-01-08
     * @param rowKeyList
     *            请求Bean实体
     *
     */
    public Map<String, Map<String, List<HBaseRecord>>> batchRowKeyGetQuery(List<String> rowKeyList) {
        String mytableName = getTableName();
        Map<String, Map<String, List<HBaseRecord>>> tableRecordsMap = new HashMap<String, Map<String, List<HBaseRecord>>>();
        if (StringUtil.isNullOrEmpty(mytableName)) {
            log.error("tableName cann't be empty please check");
            System.out.println("tableName cann't be empty please check");
            return tableRecordsMap;
        }
        List<Get> gets = new ArrayList<Get>();

        for(String rowKey:rowKeyList) {
            Get get = singleGetCondition(rowKey);
            if (get != null) {
                gets.add(get);
            } else {
                log.error("there not any condition match request params");
            }
        }
        return this.batchGetRecords(mytableName, gets);
    }


    private Get singleGetCondition(String rowKey) {
        Get resultGet = null;
        if (!StringUtil.isNullOrEmpty(rowKey)) {
            resultGet = new Get(rowKey.getBytes());
        }
        return resultGet;
    }



    /**
     * TODO 写简单的get查询方法 批量的从表中get
     *
     * @author libangqin
     * @date 2017-09-13
     * @param tableName
     *            gets 批量请求的get
     * @return Map<String, Map<String, List<HBaseRecord>>> 代表该table的表结构
     * @Detail 该方法主要提供给精确查询使用 知道、rowkey的情况下查询速度更快
     */
    // 查询的数据结构 Map<String, Map<String, List<HBaseRecord>>> dataMap
    public Map<String, Map<String, List<HBaseRecord>>> batchGetRecords(String tableName, List<Get> gets) {
        Map<String, Map<String, List<HBaseRecord>>> tableRecordsMap = new HashMap<String, Map<String, List<HBaseRecord>>>();
        try {
            if (!this.tableExists(tableName)) {
                log.info("hbase tableName:" + tableName + " is not exist");
                return tableRecordsMap;
            }
            HTableInterface table = this.getConnection().getTable(Bytes.toBytes(tableName));
            Result[] rss = table.get(gets);
            Map<String, List<HBaseRecord>> recordsMap = new HashMap<String, List<HBaseRecord>>();;
            for (Result rs : rss) {
                List<HBaseRecord> recordList = this.getResult(rs);
                String rowKey = Bytes.toString(rs.getRow());
                recordsMap.put(rowKey, recordList);
            }
            tableRecordsMap.put(tableName, recordsMap);
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return tableRecordsMap;
    }



    /**
     * @param rs
     * @return
     */
    @SuppressWarnings("deprecation")
    private List<HBaseRecord> getResult(Result rs) {
        List<HBaseRecord> recordList = new ArrayList<HBaseRecord>();
        for (KeyValue kv : rs.raw()) {
            try {
                String rowKey = Bytes.toString(kv.getRow());
                String family = Bytes.toString(kv.getFamily());
                String qualifier = Bytes.toString(kv.getQualifier());
                long timestamp = kv.getTimestamp();
                String value = "";
                value = Bytes.toString(kv.getValue());
                HBaseRecord record = new HBaseRecord(rowKey, family, qualifier, value, timestamp);
                recordList.add(record);
            } catch (Exception e) {
            }
        }
        return recordList;
    }

    public boolean tableExists(String tableName) {
        try {
            if (StringUtil.isNullOrEmpty(tableName))
                throw new Exception(
                        "Judge whether the table exists from Hadoop,tableName does not allow for Null Or Empty");
            List<String> tables = this.getTableNames();
            if (tables != null && tables.size() > 0 && tables.contains(tableName))
                return true;
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return false;
    }

    /**
     * @author libangqin
     * @param
     * @return List<String> tableNames
     * @detail 该方法主要是查询hbase服务器中所有的表名
     */
    public List<String> getTableNames() {
        try {
            if (this.tableNames == null || this.tableNames.size() <= 0) {
                synchronized (lockObj) {
                    if (this.tableNames == null || this.tableNames.size() <= 0) {
                        @SuppressWarnings("deprecation")
                        String[] list = getAdmin().getTableNames();
                        if (list != null && list.length > 0) {
                            this.tableNames.addAll(Arrays.asList(list));
                        }
                    }
                }
            }
        } catch (Exception ex) {
            log.error(ex.getMessage());
        }
        return this.tableNames;
    }

    /**
     * 添加作为查询结果的字段到get 也就是结果过滤条件
     *
     * @param get
     * @param recordList
     * @return
     */
    public Get addColumnToGet(Get get, List<HBaseRecord> recordList) {
        for (HBaseRecord r : recordList) {
            HBaseRecord record = (HBaseRecord) r;
            if (StringUtil.isNullOrEmpty(record.getValue())) { // 作为查询结果
                if (!StringUtil.isNullOrEmpty(record.getFamily()) && !StringUtil.isNullOrEmpty(record.getQualifier()))
                    get.addColumn(Bytes.toBytes(record.getFamily()), Bytes.toBytes(record.getQualifier()));
                else if (!StringUtil.isNullOrEmpty(record.getFamily()))
                    get.addFamily(Bytes.toBytes(record.getFamily()));
            }
        }
        return get;
    }


    /**
     * 根据rowKeyList批量获取
     * */
    public JSONArray getDataListByRowKeyList(List<Map<String, Object>> dataList) {
        JSONArray dataArr = new JSONArray();
        List<String> rowKeyList = new ArrayList<String>();
        for(Map<String, Object> map : dataList){
            rowKeyList.add(String.valueOf(map.get("id")));
        }
        Map<String, Map<String, List<HBaseRecord>>> batchResultRecords = batchRowKeyGetQuery(rowKeyList);

        //放每个字段的数据
        try {
            for (Map.Entry<String, Map<String, List<HBaseRecord>>> recordsMap : batchResultRecords.entrySet()) {
                String recordsMapKey = recordsMap.getKey();
                Map<String, List<HBaseRecord>> recordsMapValue = recordsMap.getValue();
                for (Map.Entry<String, List<HBaseRecord>> recordEach : recordsMapValue.entrySet()) {
                    String rowkey1 = recordEach.getKey();
                    List<HBaseRecord> val1List = recordEach.getValue();

                    JSONObject jSONObject = new JSONObject();
                    //单条记录
                    jSONObject = parseHbaseRecord2(val1List);
                    if(jSONObject.isEmpty()){
                        log.error("---------getDataListByRowKeyList方法：hbase 获取单条记录异常 。异常rowkey:"+rowkey1);
                    }
                    dataArr.add(jSONObject);
                }
                System.out.println("recordsMapKey="+recordsMapKey);
            }

        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            log.error("---------getDataListByRowKeyList方法：hbase 批量获取异常");
        }


        return dataArr;
    }


    public JSONObject parseHbaseRecord(Map<String, List<HBaseRecord>> recordsMap){
        JSONObject colsObj = new JSONObject();
        for (Map.Entry<String, List<HBaseRecord>> recordEach : recordsMap.entrySet()) {
            String key1 = recordEach.getKey();
            List<HBaseRecord> val1List = recordEach.getValue();

            for(int i=0;i<val1List.size();i++){ //遍历List
                HBaseRecord hBaseRecord = val1List.get(i);
                //System.out.println(hBaseRecord.getString());
                if(hBaseRecord!=null){
                    String[] colsArr = hBaseRecord.getString().split(",");

                    String keyName = "";
                    String keyValue = "";
                    for(int j=0;j<colsArr.length;j++){
                        //System.out.println(colsArr[j]);
                        String[] str_jArr = colsArr[j].split("\\:");
                        if(rowKeyStr.equals(str_jArr[0]))
                            colsObj.put(rowKeyStr, str_jArr[1]); //比如：("rowKey","96.B-1008_20171115030116_1790207076.TAKE_OFF_PITCH_ROLL.123456789")
                        if(qualifierStr.equals(str_jArr[0])){
                            keyName = str_jArr[1];

                        }
                        if(valueStr.equals(str_jArr[0])){
                            if(str_jArr.length>1)
                                keyValue = String.valueOf(str_jArr[1]);
                            //如果数据的值含有冒号，比如08:00:00
                            if(str_jArr.length>2){
                                for(int k=2;k<str_jArr.length;k++){
                                    keyValue = keyValue + ":" + str_jArr[k];
                                }
                            }
                        }
                        else{
                            continue;
                        }
                    }
                    colsObj.put(keyName, keyValue);//比如：("exportFrom","-10")

                }
            }
        }
        return colsObj;
    }

    public JSONObject parseHbaseRecord2(List<HBaseRecord> val1List){
        JSONObject colsObj = new JSONObject();

        for(int i=0;i<val1List.size();i++){ //遍历List
            HBaseRecord hBaseRecord = val1List.get(i);
            //System.out.println(hBaseRecord.getString());
            if(hBaseRecord!=null){
                String[] colsArr = hBaseRecord.getString().split(",");

                String keyName = "";
                String keyValue = "";
                for(int j=0;j<colsArr.length;j++){
                    //System.out.println(colsArr[j]);
                    String[] str_jArr = colsArr[j].split("\\:");
                    if(rowKeyStr.equals(str_jArr[0]))
                        colsObj.put(rowKeyStr, str_jArr[1]); //比如：("rowKey","96.B-1008_20171115030116_1790207076.TAKE_OFF_PITCH_ROLL.123456789")
                    if(qualifierStr.equals(str_jArr[0])){
                        keyName = str_jArr[1];

                    }
                    if(valueStr.equals(str_jArr[0])){
                        if(str_jArr.length>1)
                            keyValue = String.valueOf(str_jArr[1]);
                        //如果数据的值含有冒号，比如08:00:00
                        if(str_jArr.length>2){
                            for(int k=2;k<str_jArr.length;k++){
                                keyValue = keyValue + ":" + str_jArr[k];
                            }
                        }
                    }
                    else{
                        continue;
                    }
                }
                colsObj.put(keyName, keyValue);//比如：("exportFrom","-10")

            }
        }

        return colsObj;
    }

}


