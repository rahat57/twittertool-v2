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

public class IndexUserInfluenceServer extends AbstractVerticle {
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
	 public  IndexUserInfluenceServer()  {
		

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

			router.route(HttpMethod.POST, "/indexUserInfluence").blockingHandler(this::indexUserInfluence);

		
		
	
	}

	/**
	 * use to index user influencer info for given credentials
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexUserInfluence(RoutingContext routingContext) {
		String response = "";

		ObjectMapper mapper = new ObjectMapper();

		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
		String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName").toLowerCase();
		try {

			TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {	};
			HashMap<String, Object> credentials = mapper.readValue(credentialsJson, typeRef);
			long cursor = -1;
			ArrayList<Map<String, Object>> tweets;
			String userId;
			String influenceId = null;
			String influenceScreenName = screenName.toLowerCase();
			long start = System.currentTimeMillis();
			do
				{
				tweets = this.getFollowersList(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")),screenName,cursor);
				cursor = Long.parseLong(tweets.get(0).get("nextCursor").toString()); 
				influenceId = tweets.get(0).get("id").toString().toLowerCase();
				tweets.get(0).remove("nextCursor");
//				System.err.println("time at followers "+(System.currentTimeMillis()-start));
				this.esClient(this.esHost, this.esPort).prepareUpdate("twitter","user",tweets.get(0).get("id").toString()).setDoc(tweets.get(0)).setUpsert(tweets.get(0)).setRefresh(true).execute().actionGet();
				tweets.remove(0);
				LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
			for (int i = 0; i < tweets.size(); i += bulkSize) {
				ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(tweets.subList(i, Math.min(i + bulkSize, tweets.size())));
				bulks.add(bulk);
				for (ArrayList<Map<String, Object>> tweetsList : bulks) {
					
					this.indexInESearch(tweetsList,influenceScreenName,"followers");
				}
				
				}
			} while(cursor!=0);
//			System.err.println("time after indexing followers "+(System.currentTimeMillis()-start));		
			boolean success = false;
	
		ArrayList<Map<String, Object>> userName = this.userInfo(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), "");
		String credentialScreename = userName.get(0).get("userScreenName").toString().toLowerCase();
							userId = userName.get(0).get("id").toString();
							
		ArrayList<String> credentialFollowerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), credentialScreename);
		ArrayList<String> influenceFollowerIds = this.getFriendIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);

		success = updateUserInfluencerRelation(credentialScreename, influenceScreenName, userId,credentialFollowerIds,influenceFollowerIds);

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
	
public boolean updateUserInfluencerRelation(String credentialScreeName,String influncerScreenName,String userId,ArrayList<String> credentialFollowerIds,ArrayList<String> influenceFollowerIds) throws Exception { // Update User
		
		try {
			
			// getting friend and follower ids of influenceScreenname
//			influenceFollowerIds = this.elasticsearch.searchUserIds(this.esClient(this.esHost,this.esPort),influncerScreenName, "followers");
			System.err.println("influence size "+influenceFollowerIds.size());
			// getting friend and follower ids of credentialScreenname
//			credentialFollowerIds = this.elasticsearch.searchUserIds(this.esClient(this.esHost,this.esPort),credentialScreeName, "followers");
			System.err.println("credential size "+credentialFollowerIds.size());
			
			// updating  credential user data   with common followers with influence
			ArrayList<String> followersWhoCommon = new ArrayList<String>();
			ArrayList<String> followersNonCommon = new ArrayList<String>();
			
			for (int i = 0; i < influenceFollowerIds.size(); i++) {
				int var = credentialFollowerIds.contains(influenceFollowerIds.get(i)) ? 1 : 0;
				if (var == 1) {
					followersWhoCommon.add(influenceFollowerIds.get(i));
				} else {
					followersNonCommon.add(influenceFollowerIds.get(i));
				}
			}

			Map<String, Object> updatefollowerRelation = new HashMap<String, Object>();
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"FollowerRelation",followersWhoCommon);
			updatefollowerRelation.put(credentialScreeName+influncerScreenName+"NonFollowerRelation",followersNonCommon);
			elasticsearch.getESInstance(this.esHost, this.esPort).prepareUpdate("twitter","user",userId).setDoc(updatefollowerRelation).setUpsert(updatefollowerRelation).setRefresh(true).execute().actionGet();

		
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
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
	 * use to get tweets
	 * 
	 * @param twitter
	 * @param query
	 * @return list of tweets
	 * @throws TwitterException
	 * @throws Exception
	 */

	public ArrayList<Map<String, Object>> userInfo(Twitter twitter,String screenName)
			throws Exception {
		ArrayList<Map<String, Object>> tweets = twitter4jApi.getUserInfo(twitter,screenName);
		return tweets;

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
