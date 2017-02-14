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

public class SearchUserInfluenceServer extends AbstractVerticle {
	HttpServer server;
	Router router;
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
	 public  SearchUserInfluenceServer()  {
		

			this.host = "localhost";
			this.port = 8182;
			this.elasticsearch = new ElasticsearchApi();
			this.esHost = "localhost";
			this.esPort = 9300;
			this.esIndex = "twitter";
			this.documentsSize = 1000;
		

		
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
				CorsHandler.create("*").allowedMethod(HttpMethod.OPTIONS).allowedHeader("Content-Type, Authorization"));
		// registering different route handlers
		this.registerHandlers();
		server.requestHandler(router::accept).listen(port, host);
		
	}

	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {

			router.route(HttpMethod.POST, "/searchUserInfluence").blockingHandler(this::searchUserInfluence);
	}


	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserInfluence(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String userScreenName = (routingContext.request().getParam("userScreenName") == null) ? "": routingContext.request().getParam("userScreenName").toLowerCase();
		String influenceScreenName = (routingContext.request().getParam("influenceScreenName") == null) ? "followers": routingContext.request().getParam("influenceScreenName").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "followers": routingContext.request().getParam("searchIn");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		String bit = (routingContext.request().getParam("bit") == null) ? "0": routingContext.request().getParam("bit");
		int pageNo =0;
		int	flagBit = 0;
		if (!(page.isEmpty()) || !(bit.isEmpty())) {
			 pageNo = Integer.parseInt(page);
			flagBit = Integer.parseInt(bit);
		}

		try {
			
			String [] fields = {"userScreenName"};

			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, fields, userScreenName,0, 10);
//			 System.out.println("commonIds "+documents.get(0));
			ArrayList<Object> commonIds = new ArrayList<Object>();
			ArrayList<Object> nonCommonIds = new ArrayList<Object>();
			
			if (!(documents.size()==0)) {
				commonIds = (ArrayList<Object>) documents.get(0).get(userScreenName+influenceScreenName+"FollowerRelation");
				nonCommonIds = (ArrayList<Object>) documents.get(0).get(userScreenName+influenceScreenName+"NonFollowerRelation");

		}
			String common[] = getIds(commonIds);
			String nonCommon[] = getIds(nonCommonIds);
			LinkedList<String []> commonRelationIds = chunksIds(common, 100);
			LinkedList<String []> nonCommonRelationIds = chunksIds(nonCommon, 100);
			if (flagBit ==0) {
				String [] relationIds = nonCommonRelationIds.get(pageNo);
				documents = this.elasticsearch.searchUserDocumentsNew2(elasticsearch.getESInstance(esHost, esPort),influenceScreenName,searchIn,relationIds);
				responseMap.put("status", "scusses");
				responseMap.put("nonCommon", nonCommonRelationIds.size());
				responseMap.put("documents", documents);
			} else {
				String [] relationIds = commonRelationIds.get(pageNo);
				documents = this.elasticsearch.searchUserDocumentsNew2(elasticsearch.getESInstance(esHost, esPort),influenceScreenName, searchIn,relationIds);
				responseMap.put("status", "scusses");
				responseMap.put("common", commonRelationIds.size());
				responseMap.put("documents", documents);
			}
			
			
			
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	
	 public String[] getIds(ArrayList<Object> ids){
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
	

}
