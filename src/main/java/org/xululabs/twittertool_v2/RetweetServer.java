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

public class RetweetServer extends AbstractVerticle {
	private Object lock = new Object();
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
	 public  RetweetServer()  {
		

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

			router.route(HttpMethod.POST, "/retweet").blockingHandler(this::retweet);
	
	}


	/**
	 * route for retweets
	 * 
	 * @param routingContext
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
		public void retweet(RoutingContext routingContext) {

		String response;
		ObjectMapper mapper = new ObjectMapper();
		String tweetsId = (routingContext.request().getParam("tweetsId") == null) ? "[]"
				: routingContext.request().getParam("tweetsId");
		String credentialsjson = (routingContext.request().getParam(
				"credentials") == null) ? "[]" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<ArrayList<Long>> typeRef = new TypeReference<ArrayList<Long>>() {};
			ArrayList<Long> tweetsIdsList = mapper.readValue(tweetsId, typeRef);

			TypeReference<HashMap<String, Object>> credentialsTypeReference = new TypeReference<HashMap<String, Object>>() {};
			HashMap<String, Object> credentials = mapper.readValue(credentialsjson, credentialsTypeReference);

			ArrayList<Long> retweetIds = twitter4jApi.retweet(this
					.getTwitterInstance(credentials.get("consumerKey").toString(), credentials.get("consumerSecret").toString(), credentials.get("accessToken")
							.toString(), credentials.get("accessTokenSecret")
							.toString()), tweetsIdsList);
			response = mapper.writeValueAsString(retweetIds);

		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

		/**
		 * use to get twitter instance
		 * 
		 * @param consumerKey
		 * @param consumerSecret
		 * @param accessToken
		 * @param accessTokenSecret
		 * @return
		 */

		public Twitter getTwitterInstance(String consumerKey,String consumerSecret, String accessToken, String accessTokenSecret) throws TwitterException {
			return twitter4jApi.getTwitterInstance(consumerKey, consumerSecret,
					accessToken, accessTokenSecret);
		}
}
