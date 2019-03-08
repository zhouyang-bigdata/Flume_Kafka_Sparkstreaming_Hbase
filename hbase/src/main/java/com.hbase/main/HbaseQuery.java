package com.hbase.main;

import com.alibaba.fastjson.JSONArray;
import com.hbase.hbaseUtils.HBaseQueryUtils2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @ClassName HbaseQuery
 * @Description TODO
 * @Author zhouyang
 * @Date 2019/3/6 11:14
 * @Version 1.0
 **/
public class HbaseQuery {
    public static void main(String[] args){
        List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
        Map<String, Object> map1 = new HashMap<>();
        Map<String, Object> map2 = new HashMap<>();
        Map<String, Object> map3 = new HashMap<>();

        map1.put("id", "S0_002904_1045_1");
        map2.put("id", "S0_002904_1045_2");
        map3.put("id", "S0_002904_1045_3");
        //
        dataList.add(map1);
        dataList.add(map2);
        dataList.add(map3);

        //获取到了rowkey,从hbase中查出数据
        JSONArray jsonArray = new JSONArray();
        HBaseQueryUtils2 hBaseQueryUtils2 = new HBaseQueryUtils2();
        try {
            jsonArray = hBaseQueryUtils2.getDataListByRowKeyList(dataList);
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(jsonArray.toJSONString());
    }



}
