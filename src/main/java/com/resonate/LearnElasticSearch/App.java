package com.resonate.LearnElasticSearch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
/**
 * 
 * @author ajay.agarwal
 *
 */
public class App 
{
	private static final String INDEX_NAME = "inspections";
	
	private static final String INDEX_TYPE = "report";
	
    public static void main( String[] args )
    {
    	App app = new App();
    	app.execute();
    }
    /**
     * 
     */
    public void execute() {
    	RestHighLevelClient client = getClient();
    	try {           		
    		//deleteIndex(client);
    		//createIndex(client);    		
    		//singleIndexRequest(client);    		
    		//getRequest(client);
    		//deleteRequest(client);
    		//updateRequest(client);
    		//bulkRequestSimple(client);
    		//bulkRequestUsingBulkProcessor(client);
    		simpleSearch(client);
        } catch(Exception e) {
        	e.printStackTrace();
        } finally {
        	if(client !=null) {
        		try{client.close();}catch(Exception e) {}
        	}
        }
    }
    /**
     * 
     * @param client
     * @throws Exception
     */
    private void simpleSearch(RestHighLevelClient client) throws Exception {
    	SearchRequest searchRequest = new SearchRequest(INDEX_NAME); 
    	searchRequest.types(INDEX_TYPE); 
    	
    	SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); 
    	sourceBuilder.query(QueryBuilders.queryStringQuery("CHILOS")); 
    	sourceBuilder.from(0); 
    	sourceBuilder.size(5); 
    	sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); 
    	sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC)); 
    	sourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC)); 
    	
    	searchRequest.source(sourceBuilder);
    	
    	SearchResponse searchResponse = client.search(searchRequest);
    	
    	RestStatus status = searchResponse.status();    	
    	SearchHits hits = searchResponse.getHits();
    	
    	SearchHit[] searchHits = hits.getHits();
    	for (SearchHit hit : searchHits) {
    	    System.out.println(hit.getSourceAsString());
    	}
    	
    	System.out.println("status-->"+status);
    }
    
    private void bulkRequestUsingBulkProcessor(RestHighLevelClient client) throws Exception {
    	System.out.println("Executing Bulk Request using Bulk Processor");
    	BulkProcessor.Listener listener = new BulkProcessor.Listener() {
    	    @Override
    	    public void beforeBulk(long executionId, BulkRequest request) {
    	    	System.out.println("Before Bulk");
    	    	int numberOfActions = request.numberOfActions(); 
    	        System.out.println("executionId-->"+ executionId+ "  numberOfActions-->"+numberOfActions);
    	    }

    	    @Override
    	    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
    	    	System.out.println("After Bulk with no Exception");
    	        if (response.hasFailures()) { 
    	        	System.out.println("executionId-->"+ executionId);
    	        } else {    	            
    	            System.out.println("executionId-->"+ executionId+ "  response.getTook().getMillis()-->"+response.getTook().getMillis());
    	        }
    	    }
    	    @Override
    	    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
    	        System.out.println("Failed to execute bulk-->"+failure); 
    	    }
    	};
    	    	
    	BulkProcessor.Builder builder = BulkProcessor.builder(client::bulkAsync, listener);
    	builder.setBulkActions(500); 
    	builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB)); 
    	builder.setConcurrentRequests(0); 
    	builder.setFlushInterval(TimeValue.timeValueSeconds(10L)); 
    	builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3));    	
    	BulkProcessor bulkProcessor = builder.build();
      	
    	Path path = Paths.get(App.class.getClassLoader().getResource("bulk.json").toURI());
    	Files.lines(path).forEach(jsonLine->{
    		bulkProcessor.add(new IndexRequest(INDEX_NAME, INDEX_TYPE).source(jsonLine, XContentType.JSON));
    	});
    	boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
    }
    
    /**
     * 
     * @param fileName
     * @return
     * @throws Exception
     */
    private void bulkRequestSimple(RestHighLevelClient client) throws Exception {
    	System.out.println("Executing Bulk Request");
    	BulkRequest bulkRequest = new BulkRequest();
    	Path path = Paths.get(App.class.getClassLoader().getResource("bulk.json").toURI());
    	Files.lines(path).forEach(jsonLine->{
    		bulkRequest.add(new IndexRequest(INDEX_NAME, INDEX_TYPE).source(jsonLine, XContentType.JSON));
    	});
    	client.bulk(bulkRequest);
    }
    /**
     * 
     * @return
     */
    private RestHighLevelClient getClient() {
    	CredentialsProvider credentialsProvider= getCredentialsProvider();
    	RestHighLevelClient client = new RestHighLevelClient(
    	        RestClient.builder(
    	                new HttpHost("localhost", 9200, "http")).setHttpClientConfigCallback(
    	                		new RestClientBuilder.HttpClientConfigCallback() {
    	                            @Override
    	                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
    	                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    	                            }
    	                		}));    
    	return client;
		
    }
    /**
     * 
     * @param client
     * @throws Exception
     */
    private void createIndex(RestHighLevelClient client) throws Exception{
    	CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
        String mappings = getJsonAsString("inspections.json");
        createIndexRequest.mapping(INDEX_TYPE, mappings, XContentType.JSON);
        client.indices().create(createIndexRequest);
        System.out.println("Index Created-->inspections");
    }
    /**
     * 
     * @param client
     * @throws Exception
     */
    private void deleteIndex(RestHighLevelClient client) throws Exception{
    	DeleteIndexRequest request = new DeleteIndexRequest(INDEX_NAME);
    	client.indices().delete(request);
    	System.out.println("Index Deleted-->inspections");
    }
    
    /**
     * 
     * @return
     */
    private CredentialsProvider getCredentialsProvider() {
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
		        new UsernamePasswordCredentials("elastic", "elastic"));
		return credentialsProvider;
    }
    /**
     * 
     * @param client
     * @throws Exception
     */
    private void singleIndexRequest(RestHighLevelClient client) throws Exception {
    	System.out.println("SingleIndexRequest");
    	IndexRequest request = new IndexRequest(INDEX_NAME, INDEX_TYPE,  "1");   
        String json = getJsonAsString("1.json");
    	request.source(json, XContentType.JSON);
    	client.index(request);
    }
    /**
     * 
     * @param fileName
     * @return
     * @throws Exception
     */
    private String getJsonAsString(String fileName) throws Exception {
    	Path path = Paths.get(App.class.getClassLoader().getResource(fileName).toURI());
        return new String(Files.readAllBytes(path));
    }
    /**
     * 
     * @param client
     * @throws Exception
     */
    private void getRequest(RestHighLevelClient client) throws Exception {
    	System.out.println("GetRequest");
    	GetRequest getRequest = new GetRequest(INDEX_NAME, INDEX_TYPE,  "1");  
    	GetResponse getResponse = client.get(getRequest);
    	String index = getResponse.getIndex();
    	String type = getResponse.getType();
    	String id = getResponse.getId();
    	System.out.println("index-->"+index);
    	System.out.println("type-->"+type);
    	System.out.println("id-->"+id);
    	if (getResponse.isExists()) {       		
    	    String sourceAsString = getResponse.getSourceAsString();
    	    System.out.println("version-->"+getResponse.getVersion());
    	    System.out.println("Source-->");
    	    System.out.println(sourceAsString);
    	} else {
    		System.out.println("Document Does not exist");
    	}    	
    }
    /**
     * 
     * @param client
     * @throws Exception
     */
    private void deleteRequest(RestHighLevelClient client) throws Exception {
    	System.out.println("DeleteRequest");
    	DeleteRequest request = new DeleteRequest(INDEX_NAME, INDEX_TYPE,  "1"); 
    	DeleteResponse deleteResponse = client.delete(request);
    	String index = deleteResponse.getIndex();
    	String type = deleteResponse.getType();
    	String id = deleteResponse.getId();
    	long version = deleteResponse.getVersion();
    	System.out.println("index-->"+index);
    	System.out.println("type-->"+type);
    	System.out.println("id-->"+id);
    	System.out.println("version-->"+version);
    	ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
    	if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
    		System.out.println("shardInfo.getTotal() != shardInfo.getSuccessful()");
    	}
    	if (shardInfo.getFailed() > 0) {
    	    for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
    	        String reason = failure.reason(); 
    	        System.out.println("reason-->"+reason);
    	    }
    	}
    }
    /**
     * 
     * @param client
     * @throws Exception
     */
    private void updateRequest(RestHighLevelClient client) throws Exception {
    	System.out.println("UpdateRequest");
    	Map<String, Object> jsonMap = new HashMap<>();
    	jsonMap.put("business_name", "New York Express");
    	UpdateRequest request = new UpdateRequest(INDEX_NAME, INDEX_TYPE,  "1").doc(jsonMap); 
    	request.fetchSource(true);
    	UpdateResponse updateResponse = client.update(request);
    	String index = updateResponse.getIndex();
    	String type = updateResponse.getType();
    	String id = updateResponse.getId();
    	long version = updateResponse.getVersion();
    	System.out.println("index-->"+index);
    	System.out.println("type-->"+type);
    	System.out.println("id-->"+id);
    	System.out.println("version-->"+version);
    	if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
    		System.out.println("CREATED");
    	} else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
    		System.out.println("UPDATED");
    	} else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {
    		System.out.println("DELETED");
    	} else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {
    		System.out.println("NOOP");
    	}
    	GetResult result = updateResponse.getGetResult(); 
    	if (result.isExists()) {
    	    String sourceAsString = result.sourceAsString(); 
    	    System.out.println("Source-->");
    	    System.out.println(sourceAsString);
    	}
    }
}
