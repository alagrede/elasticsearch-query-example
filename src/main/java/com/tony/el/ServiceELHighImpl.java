package com.tony.el;

import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tony.el.domain.MessageIndex;

public class ServiceELHighImpl implements Serializable {

	private static final String ES_INDEX = "elindexname";
	
	private static final String ES_INDICE = "indiceCat";
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static ObjectMapper mapper = new ObjectMapper();

	public static void setMapper(ObjectMapper mapper) {
		ServiceELHighImpl.mapper = mapper;
	}
	private List<HttpHost> hosts = new ArrayList<>();
	
	public ServiceELHighImpl(List<HttpHost> hosts) {
		this.hosts = hosts;
		
		if (hosts == null || hosts.isEmpty()) {
			throw new NullPointerException("hosts must be specify");
		}
	}

	
	public List<MessageIndex> getFilesIndexed(String[] refNumbers, long startTime, long stopTime, String[] params) {
		
		List<MessageIndex> files = new ArrayList<>();

		// Query here
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		
		try {
			RestHighLevelClient client = getElasticsearchClient();

			SearchRequest searchRequest = new SearchRequest(ES_INDEX); 
			
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		
			BoolQueryBuilder qb = QueryBuilders.boolQuery();

			if (refNumbers != null) {
				for (String tailnumber : refNumbers) {
					qb.must(QueryBuilders.termQuery("refNumber", tailnumber));
				}
			}

			if (params != null) {
				BoolQueryBuilder paramsBuilder = QueryBuilders.boolQuery();
				for (String param : params) {
					paramsBuilder.must(QueryBuilders.termQuery("parameters", param));
				}
				qb.must(paramsBuilder);
			}

			BoolQueryBuilder rangeDateBuilder1 = QueryBuilders.boolQuery();
			QueryBuilder range11 = QueryBuilders.rangeQuery("dateStop").gte(startTime);
			QueryBuilder range12 = QueryBuilders.rangeQuery("dateStart").lte(stopTime);
			rangeDateBuilder1.must(range11).must(range12);
			
			BoolQueryBuilder rangeDateBuilder2 = QueryBuilders.boolQuery();
			QueryBuilder range21 = QueryBuilders.rangeQuery("dateStart").gte(startTime);
			QueryBuilder range22 = QueryBuilders.rangeQuery("dateStop").lte(stopTime);
			rangeDateBuilder2.must(range21).must(range22);
			
			BoolQueryBuilder rangeDateBuilder3 = QueryBuilders.boolQuery();
			QueryBuilder range31 = QueryBuilders.rangeQuery("dateStart").lte(stopTime);
			QueryBuilder range32 = QueryBuilders.rangeQuery("dateStop").gte(startTime);
			rangeDateBuilder3.must(range31).must(range32);
			
			BoolQueryBuilder rangeDateBuilder4 = QueryBuilders.boolQuery();
			QueryBuilder range41 = QueryBuilders.rangeQuery("dateStart").lte(startTime);
			QueryBuilder range42 = QueryBuilders.rangeQuery("dateStop").gte(stopTime);
			rangeDateBuilder4.must(range41).must(range42);
			
			BoolQueryBuilder rangeDateBuilderGeneral = QueryBuilders.boolQuery();
			BoolQueryBuilder rangeDateBuilderGlobal = rangeDateBuilderGeneral.should(rangeDateBuilder1)
																			 .should(rangeDateBuilder2)
																			 .should(rangeDateBuilder3)
																			 .should(rangeDateBuilder4);
			qb.must(rangeDateBuilderGlobal);
			
		
			
			sourceBuilder.query(qb);
			sourceBuilder.fetchSource();
			sourceBuilder.from(0); 
			sourceBuilder.size(1000); 
			sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); 
			
			searchRequest.source(sourceBuilder);
			SearchResponse searchResponse = client.search(searchRequest);
			
//			RestStatus status = searchResponse.status();
//			TimeValue took = searchResponse.getTook();
//			Boolean terminatedEarly = searchResponse.isTerminatedEarly();
//			boolean timedOut = searchResponse.isTimedOut();
//			
//			SearchHits hits = searchResponse.getHits();
//			String index = hit.getIndex();
//			String type = hit.getType();
//			String id = hit.getId();
//			float score = hit.getScore();
			
//			SearchHits hits = searchResponse.getHits();
//			for (SearchHit hit : hits.getHits()) {
//				String sourceAsString = hit.getSourceAsString();
//				Map<String, Object> sourceAsMap = hit.getSourceAsMap();
//				String documentTitle = (String) sourceAsMap.get("title");
//				List<Object> users = (List<Object>) sourceAsMap.get("user");
//				Map<String, Object> innerObject = (Map<String, Object>) sourceAsMap.get("innerObject");
//			}
//			
			
		    for (SearchHit hit : searchResponse.getHits().getHits()) {
		    	JsonNode jsonNode = mapper.readTree(hit.getSourceAsString());
		    	MessageIndex file = new MessageIndex();
		    	file.setRefNumber(jsonNode.get("refNumber").asText());
		    	file.setDateStart(jsonNode.get("dateStart").asLong());
		    	file.setDateStop(jsonNode.get("dateStop").asLong());

		    	file.setChecksum(jsonNode.get("checksum").asText());
		    	List<String> parameters = new ArrayList<>();
		    	jsonNode.get("parameters").forEach(param -> {
		    		parameters.add(param.asText());
		    	});
		    	file.setParameters(parameters.toArray(new String[0])); // 0 better than new String[list.size()] for JVM
		    	file.setUploadedDate(new Date(jsonNode.get("uploadedDate").asLong()));
		    	file.setFileName(jsonNode.get("fileName").asText());

		    	logger.info(file.toString());
		    	files.add(file);
		    }

			// on shutdown
			client.close();

		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	
		return files;
	}
	
	public String putMetastore(MessageIndex msg) {
		if (mapper == null) {
			mapper = new ObjectMapper();
		}
		try {
			String json = mapper.writeValueAsString(msg);
			IndexRequest indexRequest = new IndexRequest(ES_INDEX, ES_INDICE)
					.source(json, XContentType.JSON);
					//.opType(DocWriteRequest.OpType.CREATE);
			
			indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);  // wait the replication before continue
			
			RestHighLevelClient client = getElasticsearchClient();
			IndexResponse indexResponse = client.index(indexRequest);
			client.close();
			return indexResponse.getId();
			
		} catch(ElasticsearchException e) {
			logger.error(e.getMessage());
		} catch(IOException e) {
			logger.error(e.getMessage());
		}
		
		return null;
	}

	public void deleteMetastore(List<String> ids) {
		try {
			RestHighLevelClient client = getElasticsearchClient();

			for (String id: ids) {
				try {
					DeleteRequest request = new DeleteRequest(ES_INDEX, ES_INDICE, id);
					DeleteResponse deleteResponse = client.delete(request);
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
			
			client.close();
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}
	
	private RestHighLevelClient getElasticsearchClient() throws UnknownHostException {
		HttpHost[] array = this.hosts.stream().toArray(HttpHost[]::new);
		RestClientBuilder builder = RestClient.builder(array);
		return new RestHighLevelClient(builder);
	}


}
