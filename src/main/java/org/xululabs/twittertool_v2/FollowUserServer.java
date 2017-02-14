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

public class FollowUserServer extends AbstractVerticle {
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
	 public  FollowUserServer()  {
		

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
		
			router.route(HttpMethod.POST, "/followUser").blockingHandler(this::followRoute);

	
	}

	/**
	 * use to follow by id
	 * 
	 * @param routingContext
	 * @param routingContext
	 *            can't follow more than 1000 user in one day, total 5000 users
	 *            can be followed by a account
	 */
		public void followRoute(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String userIds = (routingContext.request().getParam("screenNames") == null) ? ""
				: routingContext.request().getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {};
			HashMap<String, String> credentials = mapper.readValue(credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> followIdsType = new TypeReference<ArrayList<String>>() {};
			ArrayList<String> followIds = mapper.readValue(userIds,followIdsType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.getFriendShip(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),
					credentials.get("accessToken"),
					credentials.get("accessTokenSecret")), followIds);
			responseMap.put("following", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

/**
	 * use to create friendship
	 * 
	 * @param twitter
	 * @param user
	 *            Screen Name
	 * @return friended data about user
	 * @throws TwitterException
	 * @throws Exception
	 */
	public ArrayList<String> getFriendShip(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException, Exception {

		return twitter4jApi.createFriendship(twitter, ScreenName);
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
