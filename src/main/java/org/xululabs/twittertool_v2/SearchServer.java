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

public class SearchServer extends AbstractVerticle {
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
	 public  SearchServer()  {
		

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
	public synchronized void start() {	
		
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


			router.route(HttpMethod.POST, "/search").blockingHandler(this::search);

	}

	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void search(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String keyword = (routingContext.request().getParam("keyword") == null) ? "cat": routingContext.request().getParam("keyword");
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "tweet": routingContext.request().getParam("searchIn");
		String size = (routingContext.request().getParam("size") == null) ? "1000": routingContext.request().getParam("size");
		String page = (routingContext.request().getParam("page") == null) ? "0": routingContext.request().getParam("page");
		int pageNo =0;
		if (! size.isEmpty() || ! page.isEmpty()) {
			 pageNo = Integer.parseInt(page);
			this.documentsSize = Integer.parseInt(size);
		}
		try {
//			System.out.println("in search route");
			String[] fields = mapper.readValue(searchIn, String[].class);
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, fields, keyword,pageNo, documentsSize);
			Map<String, Object> totalSize = elasticsearch.countSearchDocuments(elasticsearch.getESInstance(esHost, esPort),esIndex, fields, keyword);

			responseMap.put("status", "scusses");
			responseMap.put("documents", documents);
			responseMap.put("size", totalSize.get("totalSize"));
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

	}
	

}
