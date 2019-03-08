package com.hbase.main;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @ClassName HbaseCRUDTest
 * @Description TODO HBASE 增删改查example
 * @Author zhouyang
 * @Date 2019/3/8 10:07
 * @Version 1.0
 **/
public class HbaseCRUDTest {
    protected  static Connection conn;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_POS = "192.168.134.10";
    private static final String ZK_POS = "192.168.134.10";
    private static final String ZK_PORT_VALUE = "2181";

    /*** 静态构造，在调用静态方法前运行，  初始化连接对象  * */
//    static {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.rootdir", "hdfs://"+ HBASE_POS + ":8020/hbase");
//        conf.set(ZK_QUORUM, ZK_POS);
//        conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
//        //创建连接池
//        try {
//            conn = ConnectionFactory.createConnection(conf);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://"+ HBASE_POS + ":8020/hbase");
        conf.set(ZK_QUORUM, ZK_POS);
        conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
        //创建连接池
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 创建表
        HTable table = new HTable(conf, "my_test_table");
        byte[] rowKey = Bytes.toBytes("R04_002904_1045_1");
        Put put = new Put(rowKey);
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("zy"));
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("age"), Bytes.toBytes("24"));
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("number"), Bytes.toBytes(100000));
        table.put(put);
        table.close();

        //  以下方法的示例
        String tableName = "my_test_table";
        String rowKey2 = "R04_002904_1045_2";
        String columnFamily = "f1";
        String column = "xian";
        String value = "haha";
        addRow(tableName , rowKey2, columnFamily, column,  value);
    }

    /*
     * @Author zhouyang
     * @Description TODO 添加一条数据
     * @Date 19:36 2019/3/8
     * @Param [tableName, rowKey, columnFamily, column, value]
     * @return
     **/
    public static void addRow(String tableName, String rowKey, String columnFamily,
                              String column, String value)   throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));  //   通过rowkey创建一个 put 对象
        //  在 put 对象中设置 列族、列、值
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);     //  插入数据，可通过 put(List<Put>) 批量插入
        table.close();
        conn.close();
    }

    /*
     * @Author zhouyang
     * @Description TODO 获取一条数据
     * @Date 19:36 2019/3/8
     * @Param [tableName, rowKey]
     * @return
     **/
    public static void getRow(String tableName, String rowKey) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));   //  通过rowkey创建一个 get 对象
        Result result = table.get(get);         //  输出结果
        for (Cell cell : result.rawCells()) {
            System.out.println(
                    "\u884c\u952e:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                            "\u5217\u65cf:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                            "\u5217\u540d:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                            "\u503c:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                            "\u65f6\u95f4\u6233:" + cell.getTimestamp());
        }
        table.close();
        conn.close();
    }

    /*
     * @Author zhouyang
     * @Description TODO 全表扫描
     * @Date 19:36 2019/3/8
     * @Param [tableName]
     * @return
     **/
    public static void scanTable(String tableName) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();  //  创建扫描对象
        ResultScanner results = table.getScanner(scan);   //  全表的输出结果
        for (Result result : results) {
            for (Cell cell : result.rawCells()) {
                System.out.println(
                        "\u884c\u952e:" + new String(CellUtil.cloneRow(cell)) + "\t" +
                                "\u5217\u65cf:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
                                "\u5217\u540d:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
                                "\u503c:" + new String(CellUtil.cloneValue(cell)) + "\t" +
                                "\u65f6\u95f4\u6233:" + cell.getTimestamp());
            }
        }
        results.close();
        table.close();
        conn.close();
    }

    /*
     * @Author zhouyang
     * @Description TODO 删除一条数据
     * @Date 19:37 2019/3/8
     * @Param [tableName, rowKey]
     * @return
     **/
    public static void delRow(String tableName, String rowKey) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        table.close();
        conn.close();
    }

    /*
     * @Author zhouyang
     * @Description TODO 删除多条数据
     * @Date 19:37 2019/3/8
     * @Param [tableName, rows]
     * @return
     **/
    public static void delRows(String tableName, String[] rows) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        List<Delete> list = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            list.add(delete);
        }
        table.delete(list);
        table.close();
        conn.close();
    }


    /*
     * @Author zhouyang
     * @Description TODO 删除列族
     * @Date 19:37 2019/3/8
     * @Param [tableName, columnFamily]
     * @return
     **/
    public static void delColumnFamily(String tableName, String columnFamily)
            throws IOException {

        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();  // 创建数据库管理员
        hAdmin.deleteColumn(tableName, columnFamily);
        conn.close();
    }

    /*
     * @Author zhouyang
     * @Description TODO 删除数据库表
     * @Date 19:38 2019/3/8
     * @Param [tableName]
     * @return
     **/
    public static void deleteTable(String tableName) throws IOException {

        HBaseAdmin hAdmin = (HBaseAdmin) conn.getAdmin();   // 创建数据库管理员
        if (hAdmin.tableExists(tableName)) {
            hAdmin.disableTable(tableName);     //  失效表
            hAdmin.deleteTable(tableName);     //  删除表
            System.out.println("删除" + tableName + "表成功");
            conn.close();
        } else {
            System.out.println("需要删除的" + tableName + "表不存在");
            conn.close();
            System.exit(0);
        }
    }


    /*
     * @Author zhouyang
     * @Description TODO 追加插入 ,在原有的value后面追加新的value，  "a" + "bc"  -->  "abc"
     * @Date 19:37 2019/3/8
     * @Param [tableName, rowKey, columnFamily, column, value]
     * @return
     **/
    public static void appendData(String tableName, String rowKey, String columnFamily,
                                  String column, String value)  throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Append append = new Append(Bytes.toBytes(rowKey));
        append.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        table.append(append);
        table.close();
        conn.close();
    }

    /*
     * @Author zhouyang
     * @Description TODO 符合条件后添加数据
     * @Date 19:38 2019/3/8
     * @Param [tableName, rowKey, columnFamilyCheck, columnCheck, valueCheck, columnFamily, column, value]
     * @return
     **/
    public static boolean checkAndPut(String tableName, String rowKey,
                                      String columnFamilyCheck, String columnCheck,  String valueCheck,
                                      String columnFamily, String column, String value) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        boolean result = table.checkAndPut(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamilyCheck),
                Bytes.toBytes(columnCheck), Bytes.toBytes(valueCheck), put);
        table.close();
        conn.close();
        return result;
    }

    /*
     * @Author zhouyang
     * @Description TODO 符合条件后删除数据
     * @Date 19:38 2019/3/8
     * @Param [tableName, rowKey, columnFamilyCheck, columnCheck, valueCheck, columnFamily, column]
     * @return
     **/
    public static boolean checkAndDelete(String tableName, String rowKey,
                                         String columnFamilyCheck, String columnCheck,  String valueCheck,
                                         String columnFamily, String column) throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(columnFamilyCheck), Bytes.toBytes(columnCheck));
        boolean result = table.checkAndDelete(Bytes.toBytes(rowKey),
                Bytes.toBytes(columnFamilyCheck),  Bytes.toBytes(columnCheck),
                Bytes.toBytes(valueCheck), delete);
        table.close();
        conn.close();
        return result;
    }

    /*
     * @Author zhouyang
     * @Description TODO 计数器
     * @Date 19:38 2019/3/8
     * @Param [tableName, rowKey, columnFamily, column, amount]
     * @return
     **/
    public static long incrementColumnValue(String tableName, String rowKey,
                                            String columnFamily, String column, long amount)  throws IOException {

        HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
        long result = table.incrementColumnValue(Bytes.toBytes(rowKey),
                Bytes.toBytes(columnFamily), Bytes.toBytes(column), amount);
        table.close();
        conn.close();
        return result;
    }
}

