package com.amas.utils.common;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.amas.util.Logger;


import org.elasticsearch.action.search.SearchRequestBuilder;


/*
 * @Author zhouyang
 * @Description TODO 
 * @Date 10:01 2019/3/5
 * @Param 
 * @return 
 **/
public class ESClientUtils2 {
	protected static Logger logger = Logger.getLogger(ESClientUtils2.class);
    protected static TransportClient client;
    private static String es_node1_host = "";    
    private static String es_node1_port = "";
    
    static {

        String configName = "/hbase_elasticsearch.properties";
        PropertiesLoader propertiesLoader = new PropertiesLoader(configName);
        try {
        	es_node1_host = propertiesLoader.getProperty("es_node1_host");
            es_node1_port = propertiesLoader.getProperty("es_node1_port");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**   
     * @Title: setUp   
     * @Description: TODO(创建client连接)   
     * @param: @throws Exception      
     * @return: void      
     * @throws   
     */
    @Before
    public static void setUp() throws Exception {

        Settings esSettings = Settings.builder()
                .put("cluster.name", "my-application") //设置ES实例的名称
                .put("client.transport.sniff", true) //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
                .build();

        /**
         * 这里的连接方式指的是没有安装x-pack插件,如果安装了x-pack则参考{@link ElasticsearchXPackClient}
         * 1. java客户端的方式是以tcp协议在9300端口上进行通信
         * 2. http客户端的方式是以http协议在9200端口上进行通信
         */
        /*client = new PreBuiltTransportClient(esSettings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("10.0.0.12"), Integer.parseInt("9300")));*/
        
        client = new PreBuiltTransportClient(esSettings.EMPTY)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(es_node1_host), Integer.parseInt(es_node1_port)));

        System.out.println("ElasticsearchClient 连接成功");
    }


    public ESClientUtils2() throws Exception{
    	setUp();
    }

    @After
    public static void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }

    }

    /**   
     * @Title: getQueryResponse   
     * @Description: TODO(查询数据,精确查询)   
     * @param: @throws Exception      
     * @return: void      
     * @throws   
     */
    public static GetResponse getQueryResponse(String indexName, String type, String id) throws Exception{
    	setUp();
    	// 搜索数据
        GetResponse response = client.prepareGet(indexName, type, id).execute().actionGet();
        // 输出结果
        System.out.println(response.getSourceAsString());
        // 关闭client
        closeClient();
        return response;
    }
    
    /**   
     * @Title: getQueryResSource   
     * @Description: TODO(查询数据,返回Source，与getQueryResponse搭配使用)   
     * @param: @param response
     * @param: @return
     * @param: @throws Exception      
     * @return: Map<String,Object>      
     * @throws   
     */
    public static Map<String, Object> getQueryResSource(GetResponse response) throws Exception{
    	setUp();
    	//查询添加的索引
    	// Index name
    	String _index = response.getIndex();
    	// Type name
    	String _type = response.getType();
    	// Document ID (generated or not)
    	String _id = response.getId();
    	// Version (if it's the first time you index this document, you will get: 1)
    	long _version = response.getVersion();
    	Map<String, GetField> fields = response.getFields();
    	for (Entry<String, GetField> fieldsEach : fields.entrySet()) {
    		System.out.println("fieldsEach.key:"+fieldsEach.getKey() + " " + "fieldsEach.value:"+fieldsEach.getValue().toString());
    	}
    	Map<String, Object> source = response.getSource();
    	for (Entry<String, Object> sourceEach : source.entrySet()) {
    		System.out.println("sourceEach.key:"+sourceEach.getKey() + " " + "sourceEach.value:"+sourceEach.getValue());
    	}
    	// 关闭client
    	closeClient();
    	return source;
    }
    
    /**   
     * @Title: getSearchResponse   
     * @Description: TODO(查询数据,多条件查询,有分页)   
     * @param: @throws Exception      
     * @return: void      
     * @throws   
     */
    public static  Map<String, Object> getSearchResponse(String indexName, String type, Map<String, Object> requestParams) throws Exception{
    	Map<String, Object> resultMap = new HashMap<String, Object>();
    	Map<String, String> queryCondition = new HashMap<String, String>();
		Map<String, String> filterCondition = new HashMap<String, String>();
		//查询条件
		queryCondition = (Map<String, String>) requestParams.get("queryParams");
		//过滤条件
		filterCondition = (Map<String, String>) requestParams.get("filterParams");
		
    	setUp();
    	boolean matchQueryFlag = true;
    	SearchRequestBuilder searchRequestBuilder  = client.prepareSearch(indexName)
    	        .setTypes(type)
    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
    	        .setQuery(QueryBuilders.matchAllQuery());//查询所有
    	for (Entry<String, String> conditionEach : queryCondition.entrySet()) {
    		//如果某个查询字段有值，则matchQueryFlag = false
    		if(!"".equals(conditionEach.getValue())&&conditionEach.getValue() != null){
    			matchQueryFlag = false;
    			
    			
    			//----------------------------------------
        		searchRequestBuilder.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(conditionEach.getKey(), conditionEach.getValue())));
    		}
	
    	}    	
    	SearchResponse response = searchRequestBuilder
    			.setFrom(Integer.parseInt(""+requestParams.get("startRow"))).setSize(Integer.parseInt(""+requestParams.get("rowCount"))).setExplain(false)    //true,有结果解释；false，没有
    	        .get();
    	
    	closeClient();
    	// 输出结果
    	JSONArray dataArr = new JSONArray();
    	
    	SearchHits hits = response.getHits();
    	for (SearchHit searchHit : hits) {
    		System.out.println(searchHit.getSourceAsString());
            Map<String, Object> source = searchHit.getSource();
            Map<String, Object> mapObject = new HashMap<String, Object>();
    	 		
    		mapObject.put("id", source.get("rowKey"));
    		dataArr.add(mapObject);
        }
    	/*SearchHit[] hits = response.getHits().hits();
    	for(int i=0;i<hits.length;i++){
    		SearchHit hiti = hits[i];
    		Map<String, Object> mapObject = new HashMap<String, Object>();
    		//
    		JSONObject hitiJson = new JSONObject();
    		hitiJson = JSONObject.parseObject(hiti.getSourceAsString());   		
    		mapObject.put("id", hitiJson.get("rowkey"));
    		dataArr.add(mapObject);
    		System.out.println(hiti.getSourceAsString());
    	}*/
        System.out.println(response.toString());
        
        resultMap.put("queryStatus", "1001");
        resultMap.put("queryDataDesc", "");
        // 获取查询的条数
        resultMap.put("queryDataCount", response.getHits().totalHits());
        resultMap.put("queryDataContent", dataArr);
    	return resultMap;
    }
    
    /**   
     * @Title: getSearchResponse   
     * @Description: TODO(查询数据,多条件查询，不带分页)   
     * @param: @throws Exception      
     * @return: void      
     * @throws   
     */
    public static  Map<String, Object> getSearchResponseNoPage(String indexName, String type, Map<String, Object> requestParams) throws Exception{
    	Map<String, Object> resultMap = new HashMap<String, Object>();
    	Map<String, String> queryCondition = new HashMap<String, String>();
		Map<String, String> filterCondition = new HashMap<String, String>();
		//查询条件
		queryCondition = (Map<String, String>) requestParams.get("queryParams");
		//过滤条件
		filterCondition = (Map<String, String>) requestParams.get("filterParams");
		
    	setUp();
    	boolean matchQueryFlag = true;
    	SearchRequestBuilder searchRequestBuilder  = client.prepareSearch(indexName)
    	        .setTypes(type)
    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
    	        .setQuery(QueryBuilders.matchAllQuery());//查询所有
    	for (Entry<String, String> conditionEach : queryCondition.entrySet()) {
    		//如果某个查询字段有值，则matchQueryFlag = false
    		if(!"".equals(conditionEach.getValue())&&conditionEach.getValue() != null){
    			matchQueryFlag = false;
    			
    			
    			//----------------------------------------
        		searchRequestBuilder.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery(conditionEach.getKey(), conditionEach.getValue())));
    		}
	
    	}    
    	Integer rowCount = 10000;
    	SearchResponse response = searchRequestBuilder
    			.setFrom(0).setSize(rowCount).setExplain(false)    //true,有结果解释；false，没有
    	        .get();
    	
    	closeClient();
    	// 输出结果
    	JSONArray dataArr = new JSONArray();
    	
    	SearchHits hits = response.getHits();
    	for (SearchHit searchHit : hits) {
    		System.out.println(searchHit.getSourceAsString());
            Map<String, Object> source = searchHit.getSource();
            Map<String, Object> mapObject = new HashMap<String, Object>();
    	 		
    		mapObject.put("id", source.get("rowKey"));
    		dataArr.add(mapObject);
        }
    	
        System.out.println(response.toString());
        
        resultMap.put("queryStatus", "1001");
        resultMap.put("queryDataDesc", "");
        // 获取查询的条数
        resultMap.put("queryDataCount", response.getHits().totalHits());
        resultMap.put("queryDataContent", dataArr);
    	return resultMap;
    }
    
    /**   
     * @Title: getMultiSearchResponse   
     * @Description: TODO(多条件搜索)   
     * @param: @param indexName
     * @param: @param type
     * @param: @param queryCondition
     * @param: @param filterCondition
     * @param: @return
     * @param: @throws Exception      
     * @return: MultiSearchResponse      
     * @throws   
     */
    public static MultiSearchResponse getMultiSearchResponse(String indexName, String type, Map<String, Object> queryCondition, Map<String, Object> filterCondition) throws Exception{
    	setUp();
    	SearchRequestBuilder srb1 = client
    		    .prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);
    	SearchRequestBuilder srb2 = client
    		    .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

    	MultiSearchResponse sr = client.prepareMultiSearch()
    			.add(srb1)
    		    .add(srb2)
    		    .get();
    	// You will get all individual responses from MultiSearchResponse#getResponses()
    	long nbHits = 0;
    	for (MultiSearchResponse.Item item : sr.getResponses()) {
    		SearchResponse response = item.getResponse();
    		nbHits += response.getHits().getTotalHits();
    	}
    	closeClient();
    	return sr;
    }
    
    
    public static void createIndex(String indexName) throws Exception{
    	setUp();
    	client.admin().indices().prepareCreate(indexName).get();
    	closeClient();
    }
    
    /**   
     * @Title: insertData   
     * @Description: TODO(插入一条)   
     * @param: @param indexName
     * @param: @param json
     * @param: @return
     * @param: @throws Exception      
     * @return: IndexResponse      
     * @throws   
     */
    public static IndexResponse insertData(String indexName, String type, String id, JSONObject json) throws Exception{
    	
    	setUp();    	
    	IndexResponse response = client.prepareIndex(indexName, type, id)
    	        .setSource(json)
    	        .get();
    	//查询添加的索引
    	// Index name
    	String _index = response.getIndex();
    	// Type name
    	String _type = response.getType();
    	// Document ID (generated or not)
    	String _id = response.getId();
    	// Version (if it's the first time you index this document, you will get: 1)
    	long _version = response.getVersion();
    	// status has stored current instance statement.
    	RestStatus status = response.status();
    	closeClient();
    	return response;
    }
    
    /**   
     * @Title: batchInsertData   
     * @Description: TODO(批量插入)   
     * @param: @param indexName
     * @param: @param type
     * @param: @param jsonArr
     * @param: @return
     * @param: @throws Exception      
     * @return: BulkResponse      
     * @throws   
     */
    public static BulkResponse batchInsertData(String indexName, String type, JSONArray jsonArr) throws Exception{
    	setUp();
    	BulkRequestBuilder bulkRequest = client.prepareBulk();
    	for(int i=0;i<jsonArr.size();i++){
    		JSONObject jsonObj = jsonArr.getJSONObject(i);
    		//插入单条
        	bulkRequest.add(client.prepareIndex(indexName, type)
        	        .setSource(jsonObj)
        	);
    	}
    	
    	BulkResponse bulkResponse = bulkRequest.get();
    	if (bulkResponse.hasFailures()) {
    	    // 
    	    //处理失败
    		return null;
    	}
    	closeClient();
    	return bulkResponse;
    }
    
    /**   
     * @Title: updateData   
     * @Description: TODO(更新单条数据)   
     * @param: @param indexName
     * @param: @param type
     * @param: @param id
     * @param: @param json
     * @param: @return
     * @param: @throws Exception      
     * @return: UpdateResponse      
     * @throws   
     */
    public static UpdateResponse updateData(String indexName, String type, String id, JSONObject json) throws Exception{   	
    	setUp();
    	UpdateResponse updateResponse = client.prepareUpdate(indexName, type, id)
                .setDoc(json.toString(),XContentType.JSON).get();
    	closeClient();
    	return updateResponse;
    }
    
    /**   
     * @Title: deleteIndex   
     * @Description: TODO(删除索引)   
     * @param: @param indexName
     * @param: @return
     * @param: @throws Exception      
     * @return: boolean      
     * @throws   
     */
    public static boolean deleteIndex(String indexName) throws Exception{
    	setUp();
    	IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);

    	IndicesExistsResponse inExistsResponse = client.admin().indices()
    	                    .exists(inExistsRequest).actionGet();
    	if(inExistsResponse.isExists()){
    		DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName)
                    .execute().actionGet();
    		if(dResponse.isAcknowledged()){
    			closeClient();
    			return true;
    		}
    		closeClient();
    		return false;
    	}
    	
    	closeClient();
    	return false;
    }
    
    /**   
     * @Title: deleteData   
     * @Description: TODO(这里用一句话描述这个方法的作用)   
     * @param: @return
     * @param: @throws Exception      
     * @return: DeleteResponse      
     * @throws   
     */
    public static DeleteResponse deleteData(String indexName, String type, String id) throws Exception{
    	setUp();
    	DeleteResponse response = client.prepareDelete(indexName, type, id).get();
    	closeClient();
    	return response;
    }
    
    /**
     * @throws Exception    
     * @Title: DeleteByQueryAction   
     * @Description: TODO(通过查询条件删除数据)   
     * @param: @param indexName
     * @param: @param type      
     * @return: void      
     * @throws   
     */
    public static void DeleteByQueryAction(String indexName, String type) throws Exception{
    	setUp();
    	/*DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
        .filter(QueryBuilders.matchQuery("gender", "male"))      //查询            
        .source("persons")                //index(索引名)                                    
        .execute(new ActionListener<BulkByScrollResponse>() {     //回调监听     
            @Override
            public void onResponse(BulkByScrollResponse response) {
                long deleted = response.getDeleted();   //删除文档的数量                 
            }
            @Override
            public void onFailure(Exception e) {
                // Handle the exception
            }
        });*/
    	closeClient();
    }
    
    
    
    
    public static  Map<String, Object> getSearchResponsetest(String indexName, String type, Map<String, Object> requestParams) throws Exception{
    	Map<String, Object> resultMap = new HashMap<String, Object>();
    	Map<String, String> queryCondition = new HashMap<String, String>();
		Map<String, String> filterCondition = new HashMap<String, String>();
		//查询条件
		queryCondition = (Map<String, String>) requestParams.get("queryParams");
		//过滤条件
		filterCondition = (Map<String, String>) requestParams.get("filterParams");
		
    	setUp();
    	//TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("", "");
    	boolean matchQueryFlag = true;
    	MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("FLIGHT_NO", "3240");
    	for (Entry<String, String> conditionEach : queryCondition.entrySet()) {
    		matchQueryBuilder = QueryBuilders.matchQuery(conditionEach.getKey(), conditionEach.getValue());
    		if(!"".equals(conditionEach.getValue())&&conditionEach.getValue() != null)
    			matchQueryFlag = false;
    	}
    	
    	
    	SearchRequestBuilder srb1 = client
    		    .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);
    	SearchRequestBuilder srb2 = client
    		    .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);

    	MultiSearchResponse sr = client.prepareMultiSearch()
    		        .add(srb1)
    		        .add(srb2)
    		        .get();

    	// You will get all individual responses from MultiSearchResponse#getResponses()
    	long nbHits = 0;
    	for (MultiSearchResponse.Item item : sr.getResponses()) {
    		SearchResponse response = item.getResponse();
    		nbHits += response.getHits().getTotalHits();
    	}
    	
    	
    	
    	
    	//MatchQueryBuilder matchQueryBuilder2 = QueryBuilders.matchQuery("user", "李");    	
    	SearchResponse response = client.prepareSearch(indexName)
    	        .setTypes(type)
    	        .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
    	        .setQuery(matchQueryBuilder)                 // Query
    	        //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
    	        .setFrom(Integer.parseInt(""+requestParams.get("startRow"))).setSize(Integer.parseInt(""+requestParams.get("rowCount"))).setExplain(false)    //true,有结果解释；false，没有
    	        .get();
    	closeClient();
    	// 输出结果
    	JSONArray dataArr = new JSONArray();
    	
    	SearchHits hits = response.getHits();
    	for (SearchHit searchHit : hits) {
    		System.out.println(searchHit.getSourceAsString());
            Map<String, Object> source = searchHit.getSource();
            Map<String, Object> mapObject = new HashMap<String, Object>();
    	 		
    		mapObject.put("id", source.get("rowKey"));
    		dataArr.add(mapObject);
        }
    
        System.out.println(response.toString());
        
        resultMap.put("queryStatus", "1001");
        resultMap.put("queryDataDesc", "");
        // 获取查询的条数
        resultMap.put("queryDataCount", response.getHits().totalHits());
        resultMap.put("queryDataContent", dataArr);
    	return resultMap;
    }
    
    
    
    
    
    public static void main(String[] args) {
    	String indexName = "testindex";
    	String type = "person";
    	String id = "1";
    	
    	Map<String, Object> requestParams = new HashMap<String, Object>();
    	Map<String, Object> queryCondition = new HashMap<String, Object>();
    	queryCondition.put("user", "张");
    	Map<String, Object> filterCondition = new HashMap<String, Object>();
    	requestParams.put("queryParams", queryCondition);
    	requestParams.put("filterParams", filterCondition);
    	requestParams.put("startRow","0");
    	requestParams.put("rowCount","15");
        try {
        	
        	/*******************************精确查询单条数据********************************/
        	//getQueryResponse(indexName, type, id);
        	/*******************************精确查询单条数据********************************/
        	
        	/*******************************匹配查询数据********************************/
        	getSearchResponse(indexName, type, requestParams);
        	/*******************************匹配查询数据********************************/
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        /*******************************插入单条数据********************************/
       	/*JSONObject dataJson = new JSONObject();
       	dataJson.put("user", "张三");
       	dataJson.put("sex", "男");
       	dataJson.put("desc", "工程师");
        try {
			insertData(indexName, type, id, dataJson);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
        /*******************************插入单条数据********************************/
        
        
        
        /*******************************批量插入数据********************************/
        /*JSONObject jsonObj1 = new JSONObject();
        JSONObject jsonObj2 = new JSONObject();
        jsonObj1.put("user", "张三");
        jsonObj1.put("sex", "男");
        jsonObj1.put("desc", "工程师");
        jsonObj2.put("user", "李四");
        jsonObj2.put("sex", "女");
        jsonObj2.put("desc", "医生");
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(jsonObj1);
        jsonArray.add(jsonObj2);
        //批量插入
        try {
			batchInsertData(indexName, type, jsonArray);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
        /*******************************批量插入数据********************************/
        
        /*******************************更新单条数据********************************/
        /*JSONObject updatedataJson = new JSONObject();
        updatedataJson.put("user", "张三");
        updatedataJson.put("sex", "女");
        updatedataJson.put("desc", "工程师");
        try {
			updateData(indexName, type, id, updatedataJson);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}        */
        /*******************************更新单条数据********************************/
        
        /*******************************删除索引********************************/       
    	/*try {
			deleteIndex(indexName);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
        /*******************************删除索引********************************/
        
        
        /*******************************删除单条数据********************************/
    	/*try {
			deleteData(indexName, type, id);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} */    	     	
        /*******************************删除单条数据********************************/
        
    }
}
