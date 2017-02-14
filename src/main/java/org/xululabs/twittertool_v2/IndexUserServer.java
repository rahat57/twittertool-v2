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

public class IndexUserServer extends AbstractVerticle {
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
	 public  IndexUserServer()  {
		

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


			router.route(HttpMethod.POST, "/indexUser").blockingHandler(this::indexUsers);

	}

	/**
	 * use to index user info for given credentials
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexUsers(RoutingContext routingContext) {
		String response = "";

		ObjectMapper mapper = new ObjectMapper();

		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");

		try {
			TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {	};
			HashMap<String, Object> credentials = mapper.readValue(credentialsJson, typeRef);
			long cursor = -1;
			ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String,Object>>();
			String userId;
			String userScreenName = null;
			long start = System.currentTimeMillis();
			do
				{

				tweets = this.getFollowersList(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),screenName,cursor);
				cursor = Long.parseLong(tweets.get(0).get("nextCursor").toString());
				userId = tweets.get(0).get("id").toString();
				userScreenName = tweets.get(0).get("userScreenName").toString().toLowerCase();
				tweets.get(0).remove("nextCursor");
				this.esClient(this.esHost, this.esPort).prepareUpdate("twitter","user",tweets.get(0).get("id").toString()).setDoc(tweets.get(0)).setUpsert(tweets.get(0)).setRefresh(true).execute().actionGet();
			
				tweets.remove(0);
				LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
			for (int i = 0; i < tweets.size(); i += bulkSize) {
				ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(tweets.subList(i, Math.min(i + bulkSize, tweets.size())));
				bulks.add(bulk);
				for (ArrayList<Map<String, Object>> tweetsList : bulks) {
					
					this.indexInESearch(tweetsList,userScreenName,"followers");
				}
				
				}
			} while(cursor!=0);
//			System.err.println("time after indexing followers "+(System.currentTimeMillis()-start));
			long cursorvalue =-1;
			do {
				tweets = this.getFriendsList(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),screenName,cursorvalue);
				cursorvalue = Long.parseLong(tweets.get(0).get("nextCursor").toString());
				tweets.get(0).remove("nextCursor");
//				System.err.println("time after getting friends "+(System.currentTimeMillis()-start));
				this.esClient(this.esHost, this.esPort).prepareUpdate("twitter","user",tweets.get(0).get("id").toString()).setDoc(tweets.get(0)).setRefresh(true).setUpsert(tweets.get(0)).execute().actionGet();
				tweets.remove(0);
				LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
				for (int i = 0; i < tweets.size(); i += bulkSize) {
					ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(
							tweets.subList(i, Math.min(i + bulkSize, tweets.size())));
					bulks.add(bulk);
					for (ArrayList<Map<String, Object>> tweetsList : bulks) {
						this.indexInESearch(tweetsList,userScreenName,"friends");
					}

					}
				} while(cursorvalue!=0);
//			System.err.println("time after indexing friends "+(System.currentTimeMillis()-start));
			boolean success = false;
	
			ArrayList<String> followerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), userScreenName);
			ArrayList<String> friendIds = this.getFriendIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), userScreenName);

			success = this.updateUserRelation(userScreenName,userId,friendIds,followerIds);


	if (success) {
			long end = System.currentTimeMillis()-start;
			System.err.println("time taken total "+end);
			response = "{status : 'success'}";
		}
			
				
		} catch (Exception ex) {
			response = "{status: 'error', 'msg' : " + ex.getMessage() + "}";
		}
		routingContext.response().end(response);

	}


/**
	 * use to index tweets in ES
	 * 
	 * @param tweets
	 * @throws UnknownHostException
	 */
		public void indexInESearch(ArrayList<Map<String, Object>> tweets,String indexName,String type)throws UnknownHostException {
			System.out.println("index "+indexName);
			System.out.println("type "+type);
			TransportClient client = this.esClient(this.esHost, this.esPort);
			BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
			for (Map<String, Object> tweet : tweets) {
					bulkRequestBuilder.add(client.prepareUpdate(indexName.toLowerCase(),type,tweet.get("id").toString()).setDoc(tweet).setUpsert(tweet));	
				}				
			bulkRequestBuilder.setRefresh(true).execute().actionGet();
			
			client.close();	
	
	}

		public boolean updateUserRelation(String userScreeName,String userId,ArrayList<String> friendIds,ArrayList<String> followerIds) throws TwitterException{ // Update User
			
			try {	
				
			int var;
			ArrayList<String> friendsWhoCommon = new ArrayList<String>();
			ArrayList<String> friendsNonCommon = new ArrayList<String>();
			for (int i = 0; i < friendIds.size(); i++) {
				var = followerIds.contains(friendIds.get(i)) ? 1 : 0;
				if (var == 1) {
					friendsWhoCommon.add(friendIds.get(i));
				} else {
					friendsNonCommon.add(friendIds.get(i));
				}
					
				
			}
			
			ArrayList<String> followersNonCommon = new ArrayList<String>();
			
			for (int i = 0; i < followerIds.size(); i++) {
				var = friendsWhoCommon.contains(followerIds.get(i)) ? 1 : 0;
				if (var == 0) {
					followersNonCommon.add(followerIds.get(i));
	
				}
			}
			
//			System.err.println(friendNonCommon.toString());
			Map<String, Object> updateRelation = new HashMap<String, Object>();
			updateRelation.put("commonRelation",friendsWhoCommon);
			updateRelation.put("nonCommonFriends",friendsNonCommon);
			updateRelation.put("nonCommonFollowers",followersNonCommon);
			this.esClient(this.esHost, this.esPort).prepareUpdate(this.esIndex,"user",userId).setDoc(updateRelation).setUpsert(updateRelation).setRefresh(true).execute().actionGet();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * use to get friendsList
	 * 
	 * @param twitter
	 * @return friendsList
	 * @throws TwitterException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public ArrayList<Map<String, Object>> getFriendsList(Twitter twitter,String ScreenName,long cursor) throws IllegalStateException, TwitterException {

		return twitter4jApi.getFriendsList(twitter,ScreenName, cursor);
	}

	/**
	 * use to get followerslist
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException

	 * @throws Exception
	 */
	public ArrayList<Map<String, Object>> getFollowersList(Twitter twitter,String screenName,long cursor) throws IllegalStateException,TwitterException {

		return twitter4jApi.getFollowersList(twitter,screenName, cursor);
	}
	
	/**
	 * use to get followerIds
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public ArrayList<String> getFollowerIds(Twitter twitter, String screenName) throws TwitterException {

		return twitter4jApi.getFollowerIds(twitter, screenName);
	}
	
	/**
	 * use to get friendIds
	 * 
	 * @param twitter
	 * @return followersList
	 * @throws TwitterException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
	public ArrayList<String> getFriendIds(Twitter twitter, String screenName) throws TwitterException {

		return twitter4jApi.getFriendIds(twitter, screenName);
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
