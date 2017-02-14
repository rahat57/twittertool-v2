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

public class IndexTweetsServer extends AbstractVerticle {

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
	 public  IndexTweetsServer()  {
		

			this.host = "localhost";
			this.port = 8183;
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
				CorsHandler.create("*")
						.allowedMethod(HttpMethod.POST).allowedMethod(HttpMethod.OPTIONS).allowedHeader("Content-Type, Authorization"));
		// registering different route handlers
		this.registerHandlers();
		server.requestHandler(router::accept).listen(port, host);
		
	}

	/**
	 * For Registering different Routes
	 */
	public void registerHandlers() {

			router.route(HttpMethod.POST, "/indexTweets").blockingHandler(this::indexTweets);

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
