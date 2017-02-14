package org.xululabs.twittertool_v2;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.awt.Cursor;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.TwitterException;

public class SearchUserServer extends AbstractVerticle {
	 HttpServer server;
	 Router router;
	Twitter4jApi twitter4jApi;
	String host;
	int port;
	ElasticsearchApi elasticsearch;
	String esHost;
	String esIndex;
	int esPort;
	int documentsSize;
	int bulkSize = 1000;

	/**
	 * constructor use to initialize values
	 */
	 public  SearchUserServer()  {
		

			this.host = "localhost";
			this.port = 8182;
			this.twitter4jApi = new Twitter4jApi();
			this.elasticsearch = new ElasticsearchApi();
			this.esHost = "localhost";
			this.esPort = 9300;
			this.esIndex = "twitter";
			this.documentsSize = 1000;
			this.bulkSize = 500;
		

		
	}

	/**
	 * Deploying the verical
	 */
	@Override
	public void start() {	
		
		server = vertx.createHttpServer();
		router = Router.router(vertx);
		// Enable multipart form data parsing
		router.route().handler(BodyHandler.create());
		router.route().handler(
				CorsHandler.create("*").allowedMethod(HttpMethod.POST).allowedHeader("Content-Type, Authorization"));
		// registering different route handlers
		this.registerHandlers();
		server.requestHandler(router::accept).listen(port, host);
		
	}

	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {

			router.route(HttpMethod.POST, "/searchUser").blockingHandler(this::searchUser);

		
	
	}

	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUser(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "followers": routingContext.request().getParam("searchIn").toLowerCase();
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String bit = (routingContext.request().getParam("bit") == null) ? "0": routingContext.request().getParam("bit");
		int pageNo =0;
		int	flagbit =0;
		
		if (!(page.isEmpty()) || !(bit.isEmpty())) {
			 pageNo = Integer.parseInt(page);
			flagbit =Integer.parseInt(bit);
		}
		
		try {
			long start = System.currentTimeMillis();
			String[] fields={"userScreenName"};
			String nonCommonRelation = "nonCommon"+searchIn.substring(0,1).toUpperCase()+searchIn.substring(1);
			
			ArrayList<Map<String,Object>> friendIds =this.elasticsearch.searchDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, fields, indexName,0, 10);
			ArrayList<String> commonIds = null;
			ArrayList<String> nonCommonIds = null;
			if (!(friendIds.size()==0)) {
			commonIds = (ArrayList<String>) friendIds.get(0).get("commonRelation");
			nonCommonIds = (ArrayList<String>) friendIds.get(0).get(nonCommonRelation);
		}
			
			ArrayList<Map<String, Object>> commonDocuments =null;
			ArrayList<Map<String, Object>> nonCommonDocuments =null;
			String[] common = getIds(commonIds);
			String[] nonCommon = getIds(nonCommonIds);
			
			LinkedList<String []> commonRelationIds = chunksIds(common, 100);
			LinkedList<String []> nonCommonRelationids = chunksIds(nonCommon, 100);
//			System.out.println("common size "+commonRelationIds.size());
//			System.out.println("Noncommon size "+nonCommonRelationids.size());
			
			if (flagbit==0) {
				String [] relationIds = nonCommonRelationids.get(pageNo);
				nonCommonDocuments = this.elasticsearch.searchUserDocumentsNew2(elasticsearch.getESInstance(esHost, esPort),indexName,searchIn,relationIds);
				responseMap.put("status", "scusses");
				responseMap.put("nonCommon", nonCommonRelationids.size());
				responseMap.put("nonCommonDocuments", nonCommonDocuments);
				
			} else {
				String [] relationIds = commonRelationIds.get(pageNo);
				commonDocuments = this.elasticsearch.searchUserDocumentsNew2(this.esClient(esHost, esPort),indexName, searchIn,relationIds);
				responseMap.put("status", "scusses");
				responseMap.put("common", commonRelationIds.size());
				responseMap.put("commonDocuments", commonDocuments);
			}

			long end = System.currentTimeMillis()-start;
			System.out.println("time taken "+end);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	
	 public String[] getIds(ArrayList<String> ids){
		  String [] id =new String[ids.size()];
		  for (int i=0;i< ids.size();i++) {
			  
			id[i] = ids.get(i).toString();
		}
		  
		  return id;
	  }
	  
	  public  LinkedList<String[]> chunksIds(String [] bigList, int n) {
			int partitionSize = n;
			LinkedList<String[]> partitions = new LinkedList<String[]>();
			for (int i = 0; i < bigList.length; i += partitionSize) {
				String[] bulk = Arrays.copyOfRange(bigList, i,
						Math.min(i + partitionSize, bigList.length));
				partitions.add(bulk);
			}

			return partitions;
		}
	
	  /**
		 * use to get es instance
		 * 
		 * @param esHost
		 * @param esPort
		 * @return
		 * @throws UnknownHostException
		 */
			public TransportClient esClient(String esHost, int esPort)
				throws UnknownHostException {
			TransportClient client = new TransportClient()
					.addTransportAddress(new InetSocketTransportAddress(esHost,
							esPort));
			return client;
		}
	  
}
