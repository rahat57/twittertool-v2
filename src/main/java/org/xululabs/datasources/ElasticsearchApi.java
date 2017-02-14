package org.xululabs.datasources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

public class ElasticsearchApi {
	
	/**
	 * use to get elasticsearch instance
	 * 
	 * @param host
	 * @param port
	 * @return client
	 */
	 public TransportClient getESInstance(String host, int port)
			throws Exception {
		//synchronized (lock) {
			TransportClient client = new TransportClient()
			.addTransportAddress(new InetSocketTransportAddress(host, port));
	return client;
		//}
		
	}
	 
	 public Map<String, Object> countSearchDocuments(TransportClient client, String index, String fields[],String keyword) throws Exception {
			
			
		Map<String, Object> totalSize = new HashMap<String,Object>();
				
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (String field : fields) {
			boolQuery.should(QueryBuilders.matchPhraseQuery(field, keyword));
		}
		SearchResponse response = client.prepareSearch(index).setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery)
			.setFrom(0).setSize(1).setExplain(true).execute()
			.actionGet();
			totalSize.put("totalSize", response.getHits().getTotalHits());
	// close client
	client.close();
	return totalSize;
}
	 public Map<String, Object> countSearchUserDocuments(TransportClient client, String index, String fields) throws Exception {
		 
			Map<String, Object> totalSize = new HashMap<String,Object>();
			SearchResponse response = client.prepareSearch(index).setTypes(fields).setSearchType(SearchType.QUERY_THEN_FETCH)
				.setFrom(0).setSize(1).setExplain(true).execute()
				.actionGet();
				totalSize.put("totalSize", response.getHits().getTotalHits());
		// close client
		client.close();
		return totalSize;
	}

	public ArrayList<Map<String, Object>> searchDocuments(TransportClient client, String index, String fields[],String keyword,int page, int documentsSize) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (String field : fields) {
			boolQuery.should(QueryBuilders.matchPhraseQuery(field, keyword));
		}
		SearchResponse response = client.prepareSearch(index)
				.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery)
				.setFrom(page).setSize(documentsSize).setExplain(true).execute()
				.actionGet();
		SearchHit[] results = response.getHits().getHits();

		for (SearchHit hit : results) {

			Map<String, Object> result = hit.getSource(); // the retrieved document
			documents.add(result);
		}
		
		// close client
		client.close();
		return documents;
	}

	public ArrayList<Map<String, Object>> searchUserDocumentsNew(TransportClient client, String index, String field,String[] ids,int page, int documentsSize) throws Exception {
		
		ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		for (String id : ids) {
			boolQuery.should(QueryBuilders.matchPhraseQuery("_id", id));
		}
		SearchResponse response = client.prepareSearch(index)
				.setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(boolQuery)
				.setFrom(page).setSize(documentsSize).setExplain(true).execute()
				.actionGet();
		SearchHit[] results = response.getHits().getHits();

		for (SearchHit hit : results) {

			Map<String, Object> result = hit.getSource(); // the retrieved document
			documents.add(result);
			}
		
		return documents;
	}
	
		public ArrayList<Map<String, Object>> searchUserDocumentsNew2(TransportClient client, String index, String type,String[] ids) throws Exception {
			
			ArrayList<Map<String, Object>> documents = new ArrayList<Map<String, Object>>();
			MultiGetResponse multiGetItemResponses = client.prepareMultiGet()
				    .add(index, type, ids)           
//				    .add("orbittest2", "friends", nonCommonfriends)  
				    .get();

				for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
				    GetResponse response = itemResponse.getResponse();
				    if (response.isExists()) {                      
				        Map<String, Object> map = response.getSourceAsMap(); 
				        documents.add(map);
				    }
				}
			
			// close client
			client.close();
			return documents;
		}
		
	public ArrayList<String> searchUserIds(TransportClient client, String index, String type) throws Exception {
		
		ArrayList<String> documents = new ArrayList<String>();

//		System.out.println("index "+index);
//		System.err.println("type "+type);
			boolean exist = this.exist(client, index);
//			System.err.println(exist);
			
			if (exist) {
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setSearchType(SearchType.QUERY_THEN_FETCH).setFrom(0)
				.setSize(900000)
				.execute().actionGet();
				for (SearchHit hit : response.getHits()) {
					
					Map<String, Object> result = hit.getSource(); // the retrieved
//					System.err.println(result);
					documents.add(result.get("id").toString()); //.get("id").toString()
				}
			}
		
		
		// close client
		client.close();
		return documents;
	}
	public  boolean exist(TransportClient client,String INDEX_NAME) {
		
		IndexMetaData indexMetaData = client.admin().cluster().state(Requests.clusterStateRequest())
	            .actionGet()
	            .getState()
	            .getMetaData()
	            .index(INDEX_NAME);

	    return (indexMetaData != null);	
	   
	}


}
