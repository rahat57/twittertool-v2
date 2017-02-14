package org.xululabs.twittertool_v2;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.net.NetServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;

import java.io.IOException;
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
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.xululabs.datasources.ElasticsearchApi;
import org.xululabs.datasources.Twitter4jApi;

import twitter4j.Twitter;
import twitter4j.TwitterException;

public class DeployServer extends AbstractVerticle {

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
	 public  DeployServer()  {
			this.host = "localhost";
			this.port = 8182;
			this.twitter4jApi = new Twitter4jApi();
			this.elasticsearch = new ElasticsearchApi();
			this.esHost = "localhost";
			this.esPort = 9300;
			this.esIndex = "twitter";
			this.documentsSize = 1000;
			this.bulkSize = 500;
		System.err.println("in constructor ");
	}

	/**
	 * Deploying the verical
	 */
	@Override
	public void start() {	
//		Future<HttpServer> httpServerFuture = Future.future();
	
			System.err.println("in start ");
		server = vertx.createHttpServer();
		router = Router.router(vertx);
		// Enable multipart form data parsing
		router.route().handler(BodyHandler.create());
		router.route().handler(
				CorsHandler.create("*").allowedMethod(HttpMethod.GET)
						.allowedMethod(HttpMethod.POST)
						.allowedMethod(HttpMethod.OPTIONS)
						.allowedHeader("Content-Type, Authorization"));
		// registering different route handlers
		this.registerHandlers();
//		server.listen(httpServerFuture.completer());
//		Future<NetServer> netServerFuture = Future.future();
//		netServer.listen(netServerFuture.completer());
//		CompositeFuture.all(Arrays.asList(future1, future2, future3));
		server.requestHandler(router::accept).listen(port, host);
		
	}

	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {
		System.err.println("in register ");
		router.route(HttpMethod.GET, "/").blockingHandler(this::welcomeRoute);
		router.route(HttpMethod.POST, "/search").blockingHandler(this::search);
		router.route(HttpMethod.POST, "/searchUser").blockingHandler(this::searchUser);
		router.route(HttpMethod.POST, "/searchUserRelation").blockingHandler(this::searchUserRelation);
		router.route(HttpMethod.POST, "/searchUserInfluence").blockingHandler(this::searchUserInfluence);
		router.route(HttpMethod.POST, "/indexTweets").blockingHandler(this::indexTweets);
		router.route(HttpMethod.POST, "/userInfo").blockingHandler(this::userInfoRoute);
		router.route(HttpMethod.POST, "/indexUser").blockingHandler(this::indexUsers);
		router.route(HttpMethod.POST, "/indexUserInfluence").blockingHandler(this::indexUserInfluence);
		router.route(HttpMethod.POST, "/retweet").blockingHandler(this::retweet);
		router.route(HttpMethod.POST, "/muteUser").blockingHandler(this::muteRoute);
		router.route(HttpMethod.POST, "/blockUser").blockingHandler(this::blockRoute);		
		router.route(HttpMethod.POST, "/followUser").blockingHandler(this::followRoute);
		router.route(HttpMethod.POST, "/unfollowUser").blockingHandler(this::unfollowRoute);
		router.route(HttpMethod.POST, "/testing").blockingHandler(this::test);
	

}

	/**
	 * welcome route
	 * 
	 * @param routingContext
	 */

	public void welcomeRoute(RoutingContext routingContext) {
		routingContext.response().end("<h1>Welcome To Twitter Tool</h1>");
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
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void searchUserRelation(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String indexName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName").toLowerCase();
		String keyword = (routingContext.request().getParam("keyword") == null) ? "": routingContext.request().getParam("keyword").toLowerCase();
		String searchIn = (routingContext.request().getParam("searchIn") == null) ? "userScreenName": routingContext.request().getParam("searchIn");
		
		try {
//			System.out.println("in search route");
			String[] fields = mapper.readValue(searchIn, String[].class);
			ArrayList<Map<String, Object>> documents = this.elasticsearch.searchDocuments(this.esClient(esHost, esPort),indexName, fields, keyword,0, 10);
			responseMap.put("status", "scusses");
			responseMap.put("documents", documents);
			response = mapper.writeValueAsString(responseMap);

		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

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
			ArrayList<Object> commonIds = null;
			ArrayList<Object> nonCommonIds = null;
			if (!(friendIds.size()==0)) {
			commonIds = (ArrayList<Object>) friendIds.get(0).get("commonRelation");
			nonCommonIds = (ArrayList<Object>) friendIds.get(0).get(nonCommonRelation);
		}
			
			ArrayList<Map<String, Object>> commonDocuments =null;
			ArrayList<Map<String, Object>> nonCommonDocuments =null;
			String[] common = getIds(commonIds);
			String[] nonCommon = getIds(nonCommonIds);
			
			LinkedList<String []> commonRelationIds = chunksIds(common, 100);
			LinkedList<String []> nonCommonRelationids = chunksIds(nonCommon, 100);
			
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
		ArrayList<String> influenceFollowerIds = this.getFollowerIds(this.getTwitterInstance((String) credentials.get("consumerKey"),(String) credentials.get("consumerSecret"),(String) credentials.get("accessToken"),(String) credentials.get("accessTokenSecret")), influenceScreenName);

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
	 * use to index tweets for given keyword
	 * 
	 * @param routingContext
	 * @throws Exception
	 */
	public void indexTweets(RoutingContext routingContext) {
		String response = "";
		int keywordsIndex = 0;
		int credentialsIndex = 0;
		ObjectMapper mapper = new ObjectMapper();
		String keywordsJson = (routingContext.request().getParam("keywords") == null) ? "['cricket', 'football']": routingContext.request().getParam("keywords");
		String credentialsJson = (routingContext.request().getParam("credentials") == null) ? "[]" : routingContext.request().getParam("credentials");

		try {
//			System.out.println("in index route");
			String[] keywords = mapper.readValue(keywordsJson, String[].class);
			TypeReference<ArrayList<HashMap<String, Object>>> typeRef = new TypeReference<ArrayList<HashMap<String, Object>>>() {};
			ArrayList<HashMap<String, Object>> credentials = mapper.readValue(credentialsJson, typeRef);
			if (keywords.length == 0 || credentials.size() == 0) {
				response = "correctly pass keywords or credentials ";
			} else {

				while (keywordsIndex < keywords.length) {

					if (credentialsIndex > credentials.size() - 1)
						credentialsIndex = 0;

					Map<String, Object> credentialsMap = credentials.get(credentialsIndex);
					
					ArrayList<Map<String, Object>> tweets = this.searchTweets(this.getTwitterInstance((String) credentialsMap.get("consumerKey"),(String)credentialsMap.get("consumerSecret"),(String) credentialsMap.get("accessToken"),(String) credentialsMap
					.get("accessTokenSecret")),	keywords[keywordsIndex]);
					
					if (tweets.size() == 0) {
						keywordsIndex--;
					}
					
					LinkedList<ArrayList<Map<String, Object>>> bulks = new LinkedList<ArrayList<Map<String, Object>>>();
					for (int i = 0; i < tweets.size(); i += bulkSize) {
						ArrayList<Map<String, Object>> bulk = new ArrayList<Map<String, Object>>(
								tweets.subList(i,
										Math.min(i + bulkSize, tweets.size())));
						bulks.add(bulk);
					}
					
					for (ArrayList<Map<String, Object>> tweetsList : bulks) {
						this.indexInES(tweetsList);
					}
					keywordsIndex++;
					credentialsIndex++;

				}
			}
//			System.out.println("in index route after");
			response = "{status : 'success'}";
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
	public void indexInES(ArrayList<Map<String, Object>> tweets)
			throws UnknownHostException {
		TransportClient client = this.esClient(this.esHost, this.esPort);

		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		for (Map<String, Object> tweet : tweets) {
			bulkRequestBuilder.add(client
					.prepareUpdate("twitter", "tweets",
							tweet.get("id").toString()).setDoc(tweet)
					.setUpsert(tweet));

		}
		bulkRequestBuilder.setRefresh(true).execute().actionGet();

		client.close();
	}
	
	
	
	/**
	 * use to search documents
	 * 
	 * @param routingContext
	 */
	public void test(RoutingContext routingContext) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		ObjectMapper mapper = new ObjectMapper();
		String screenName = (routingContext.request().getParam("screenName") == null) ? "": routingContext.request().getParam("screenName");
		String keyword = (routingContext.request().getParam("keyword") == null) ? "": routingContext.request().getParam("keyword");
		
		try {
			
//			this.updateUserRelation("orbittest2", "799593858585739300");
//			this.updateUserInfluencerRelation("orbittest2", "mohsinmalvi19", "799593858585739300");
//			System.out.println("in search route");
			long start = System.currentTimeMillis();
			ArrayList<String> documents = this.elasticsearch.searchUserIds(elasticsearch.getESInstance(esHost, esPort),screenName, keyword);
//			Map<String, Object> totalSize = elasticsearch.countSearchUserDocuments(elasticsearch.getESInstance(esHost, esPort),screenName, keyword);

			responseMap.put("status", "scusses");
			responseMap.put("documents", documents);
			responseMap.put("size", documents.size());
			response = mapper.writeValueAsString(responseMap);
				System.err.println("time taken "+(System.currentTimeMillis()-start));
		} catch (Exception e) {
			response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
					+ "}";
		}

		routingContext.response().end(response);

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
			ArrayList<String> commonIds = new ArrayList<String>();
			ArrayList<String> nonCommonIds = new ArrayList<String>();
			
			if (documents.size() !=0) {
				commonIds =(ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"FollowerRelation");
				nonCommonIds = (ArrayList<String>) documents.get(0).get(userScreenName+influenceScreenName+"NonFollowerRelation");

		}
			String common[] = getArrayIds(commonIds);
			String nonCommon[] = getArrayIds(nonCommonIds);
			LinkedList<String []> commonRelationIds = chunksIds(common, 100);
			LinkedList<String []> nonCommonRelationIds = chunksIds(nonCommon, 100);
			if (flagBit ==0) {
				String [] relationIds = nonCommonRelationIds.get(pageNo);
				if (nonCommonRelationIds.size()==0) {
					responseMap.put("status", "no result");
					
				}
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
	 public String[] getArrayIds(ArrayList<String> ids){
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
		String tweetsId = (routingContext.request().getParam("tweetsId") == null) ? "[]": routingContext.request().getParam("tweetsId");
		String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "[]" : routingContext.request().getParam("credentials");
		try {
			TypeReference<ArrayList<Long>> typeRef = new TypeReference<ArrayList<Long>>() {};
			ArrayList<Long> tweetsIdsList = mapper.readValue(tweetsId, typeRef);

			TypeReference<HashMap<String, Object>> credentialsTypeReference = new TypeReference<HashMap<String, Object>>() {};
			HashMap<String, Object> credentials = mapper.readValue(credentialsjson, credentialsTypeReference);

			ArrayList<Long> retweetIds = twitter4jApi.retweet(this.getTwitterInstance(credentials.get("consumerKey").toString(), credentials.get("consumerSecret").toString(), credentials.get("accessToken")
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
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
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
	 * use to unfollow by id
	 * 
	 * @param routingContext
	 * @param credentials
	 * @param userIds
	 *            can't follow more than 1000 user in one day, total 5000 users
	 *            can be followed by a account
	 */
		public void unfollowRoute(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String userIds = (routingContext.request().getParam("screenNames") == null) ? ""
				: routingContext.request().getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {	};
			HashMap<String, String> credentials = mapper.readValue(credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> followIdsType = new TypeReference<ArrayList<String>>() {};
			ArrayList<String> followIds = mapper.readValue(userIds,followIdsType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.destroyFriendShip(this.getTwitterInstance(credentials.get("consumerKey"),
							credentials.get("consumerSecret"),
							credentials.get("accessToken"),
							credentials.get("accessTokenSecret")), followIds);
			responseMap.put("unfollowing", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

	/**
	 * use to mute by using screenName
	 * 
	 * @param routingContext
	 * @param credentials
	 * @param ScreenName
	 *            return name of those on which action has been taken
	 */
		public void muteRoute(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String friendsListJson = (routingContext.request().getParam("screenNames") == null) ? "" : routingContext.request()
				.getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {
			};
			HashMap<String, String> credentials = mapper.readValue(
					credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> friendsListType = new TypeReference<ArrayList<String>>() {
			};
			ArrayList<String> friendsList = mapper.readValue(friendsListJson,
					friendsListType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.muteUser(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),credentials.get("accessToken"),credentials.get("accessTokenSecret")), friendsList);
			responseMap.put("muted", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}

	/**
	 * use to block by using screenName
	 * 
	 * @param routingContext
	 * @param credentials
	 * @param ScreenName
	 *            return name of those on which action has been taken
	 */
		public void blockRoute(RoutingContext routingContext) {
		ObjectMapper mapper = new ObjectMapper();
		HashMap<String, Object> responseMap = new HashMap<String, Object>();
		String response;
		String friendsListJson = (routingContext.request().getParam(
				"screenNames") == null) ? "" : routingContext.request()
				.getParam("screenNames");
		String credentialsJson = (routingContext.request().getParam(
				"credentials") == null) ? "" : routingContext.request()
				.getParam("credentials");
		try {
			TypeReference<HashMap<String, Object>> credentialsType = new TypeReference<HashMap<String, Object>>() {
			};
			HashMap<String, String> credentials = mapper.readValue(
					credentialsJson, credentialsType);
			TypeReference<ArrayList<String>> friendsListType = new TypeReference<ArrayList<String>>() {
			};
			ArrayList<String> friendsList = mapper.readValue(friendsListJson,
					friendsListType);
			ArrayList<String> FreindshipResponse = null;
			FreindshipResponse = this.blockUser(this.getTwitterInstance(credentials.get("consumerKey"),credentials.get("consumerSecret"),credentials.get("accessToken"),credentials.get("accessTokenSecret")), friendsList);
			responseMap.put("blocked", FreindshipResponse);
			response = mapper.writeValueAsString(responseMap);
		} catch (Exception ex) {
			response = "{\"status\" : \"error\", \"msg\" :" + ex.getMessage()
					+ "}";
		}
		routingContext.response().end(response);
	}
		
		/**
		 * use to get userInfo
		 * 
		 * @param routingContext
		 */
		public void userInfoRoute(RoutingContext routingContext) {
			Map<String, Object> responseMap = new HashMap<String, Object>();
			String response;
			ObjectMapper mapper = new ObjectMapper();
			String credentialsjson = (routingContext.request().getParam("credentials") == null) ? "" : routingContext.request().getParam("credentials");
			String screenName = (routingContext.request().getParam("screenName") == null) ? "" : routingContext.request().getParam("screenName");

			try {
				TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {	};
				HashMap<String, Object> credentials = mapper.readValue(credentialsjson, typeRef);
				ArrayList<Map<String, Object>> documents;
				documents = this.userInfo(this.getTwitterInstance(credentials.get("consumerKey").toString(),credentials.get("consumerSecret").toString(),credentials.get("accessToken").toString(),credentials.get("accessTokenSecret").toString()),screenName);
				responseMap.put("userInfo", documents);
				responseMap.put("size", documents.size());
				response = mapper.writeValueAsString(responseMap);

			} catch (Exception e) {
				response = "{\"status\" : \"error\", \"msg\" :" + e.getMessage()
						+ "}";
			}

			routingContext.response().end(response);

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
	 * use to Mute User
	 * 
	 * 
	 * @param credentials
	 * @param screenName
	 * @return name of those on which actio has benn taken
	 */
		public ArrayList<String> muteUser(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException {

		return twitter4jApi.muteUser(twitter, ScreenName);
	}

	/**
	 * use to Block User
	 * 
	 * @param credentials
	 * @param screenName
	 * @return name of those on which actio has benn taken
	 */
	public ArrayList<String> blockUser(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException {

		return twitter4jApi.blockUser(twitter, ScreenName);
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

	public ArrayList<Map<String, Object>> searchTweets(Twitter twitter,
			String keyword) throws Exception {
		ArrayList<Map<String, Object>> tweets = twitter4jApi.search(twitter,
				keyword);
		return tweets;

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

	public ArrayList<Map<String, Object>> search(Twitter twitter,
			String keyword) throws Exception {
		ArrayList<Map<String, Object>> tweets = twitter4jApi.search(twitter,
				keyword);
		return tweets;

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
	 * use to create friendship
	 * 
	 * @param twitter
	 * @param user
	 *            Screen Name
	 * @return friended data about user
	 * @throws TwitterException
	 * @throws Exception
	 */
	public ArrayList<String> destroyFriendShip(Twitter twitter,
			ArrayList<String> ScreenName) throws TwitterException, Exception {

		return twitter4jApi.destroyFriendship(twitter, ScreenName);
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
