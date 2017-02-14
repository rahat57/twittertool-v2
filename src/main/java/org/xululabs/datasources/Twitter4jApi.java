package org.xululabs.datasources;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import twitter4j.Friendship;
import twitter4j.IDs;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Relationship;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter4jApi {

	/**
	 * use to get twitter instance
	 * 
	 * @param consumerKey
	 * @param consumerSecret
	 * @param accessToken
	 * @param accessTokenSecret
	 * @return twitter
	 */

	public Twitter getTwitterInstance(String consumerKey,
			String consumerSecret, String accessToken, String accessTokenSecret) {
		Twitter twitter = null;
		try {
			ConfigurationBuilder cb = new ConfigurationBuilder();
			cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
					.setOAuthConsumerSecret(consumerSecret)
					.setOAuthAccessToken(accessToken)
					.setOAuthAccessTokenSecret(accessTokenSecret);
			TwitterFactory tf = new TwitterFactory(cb.build());
			twitter = tf.getInstance();
		} catch (Exception e) {

			e.printStackTrace();
		}

		return twitter;
	}

	/**
     * use to search in twitter for given keyword
     * @param twitter
     * @param keyword
     * @return
     * @throws Exception
     */
	public ArrayList<Map<String, Object>> search(Twitter twitter, String keyword)
			throws Exception {
		int searchResultCount = 0;
		long lowestTweetId = Long.MAX_VALUE;
		int tweetsCount = 0;
		int requestsCount = 0;
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		Query query = new Query(keyword);
		query.setCount(100);
		do {
			QueryResult queryResult;
			try {
				queryResult = twitter.search(query);
				searchResultCount = queryResult.getTweets().size();
				requestsCount++;
				for (Status tweet : queryResult.getTweets()) {
					Map<String, Object> tweetInfo = new HashMap<String, Object>();
					tweetInfo.put("id", tweet.getId());
					tweetInfo.put("tweet", tweet.getText());
					tweetInfo.put("screenName", tweet.getUser().getScreenName());
					tweetInfo.put("name", tweet.getUser().getName());
					tweetInfo.put("retweetCount", tweet.getRetweetCount());
					tweetInfo.put("followersCount", tweet.getUser().getFollowersCount());
					tweetInfo.put("user_image", tweet.getUser().getProfileImageURL());
					tweetInfo.put("description", tweet.getUser().getDescription());
					tweetInfo.put("user_location", tweet.getUser().getLocation());
					tweetInfo.put("tweet_location", tweet.getGeoLocation());
					tweets.add(tweetInfo);
					tweetsCount++;
					if (tweet.getId() < lowestTweetId) {
						lowestTweetId = tweet.getId();
						query.setMaxId(lowestTweetId);
					}
				}
			} catch (TwitterException e) {
				twitter = null;
				break;
			}
			
		} while (true);
		
		return tweets;

	}
	/**
	 * use to retweet
	 * 
	 * @param twitter
	 * @param tweetIds
	 * @throws TwitterException
	 */
	public ArrayList<Long> retweet(Twitter twitter, ArrayList<Long> tweetIds) throws TwitterException {
		ArrayList<Long> retweetIds = new ArrayList<Long>();
		int index = 0;
		while (twitter.getRetweetsOfMe().getRateLimitStatus().getLimit() > 0 && index < tweetIds.size()) {
			try {
				twitter.retweetStatus(tweetIds.get(index));
				retweetIds.add(tweetIds.get(index));
			} catch (Exception ex) {
              
			}
			index++;
		}
		
		twitter = null;
		return retweetIds;

	}

	/**
	   * use to get info about user
	   * 
	   * @param twitter
	   * @param ScreenName
	   * @return blocked, info of the person
	   * @throws TwitterException
	   */
	  public ArrayList<Map<String, Object>> getUserInfo(Twitter twitter,String screenName) throws TwitterException {

	    ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
	    try {          
	    	if (screenName.isEmpty()) {
				screenName = twitter.getScreenName();
			}
	      Map<String, Object> cursorValue = null;      
	      User followersCount = twitter.showUser(screenName);
	      cursorValue = new HashMap<String, Object>();
	        cursorValue.put("userScreenName", followersCount.getScreenName());
			cursorValue.put("friendsCount",followersCount.getFriendsCount());
			cursorValue.put("followersCount",followersCount.getFollowersCount());	
			cursorValue.put("id", followersCount.getId());
			cursorValue.put("userImage", followersCount.getProfileImageURL());
			cursorValue.put("description", followersCount.getDescription());
			cursorValue.put("tweetsCount", followersCount.getStatusesCount());
			cursorValue.put("userLocation", followersCount.getLocation());
	        tweets.add(cursorValue);

	    } catch (Exception e) {

	      e.printStackTrace();
	    }

	    return tweets;
	  }
	
	/**
	 * use to create friendship
	 * 
	 * @param twitter
	 * @param ScreenName
	 * @return friended, info of the person
	 * @throws TwitterException
	 */
	public ArrayList<String> createFriendship(Twitter twitter,ArrayList<String> ScreenName) throws TwitterException {

		ArrayList<String> tweets = new ArrayList<String>();
		
		
		try {
			
			for (String  user: ScreenName) {
				
				User tweet = twitter.createFriendship(user);
				tweets.add(tweet.getScreenName());
				
			} 

		} catch (Exception e) {

			e.printStackTrace();
		}

		return tweets;
	}
	/**
	 * use to destroy friendship
	 * 
	 * @param twitter
	 * @param ScreenName
	 * @return UnFriended info of the person
	 * @throws TwitterException
	 */
	public ArrayList<String> destroyFriendship(Twitter twitter,ArrayList<String> ScreenName) throws TwitterException {

		ArrayList<String> tweets = new ArrayList<String>();
		try {
			
			Map<String, Object> tweetInfo = null;
			for (String  user: ScreenName) {
				User tweet = twitter.destroyFriendship(user);
				tweets.add(tweet.getScreenName());	
			} 

		} catch (Exception e) {

			e.printStackTrace();
		}

		return tweets;
	}
	
	/**
	 * use to block user
	 * 
	 * @param twitter
	 * @param ScreenName
	 * @return blocked, info of the person
	 * @throws TwitterException
	 */
	public ArrayList<String> blockUser(Twitter twitter,ArrayList<String> ScreenName) throws TwitterException {

		ArrayList<String> tweets = new ArrayList<String>();
		try {

			for (String  user: ScreenName) {
				User tweet = twitter.createBlock(user);
				tweets.add(tweet.getScreenName());
			} 

		} catch (Exception e) {

			e.printStackTrace();
		}

		return tweets;
	}
	/**
	 * use to block user
	 * 
	 * @param twitter
	 * @param ScreenName
	 * @return blocked, info of the person
	 * @throws TwitterException
	 */
	public ArrayList<String> muteUser(Twitter twitter,ArrayList<String> ScreenName) throws TwitterException {

		ArrayList<String> tweets = new ArrayList<String>();
		try {
			
			for (String  user: ScreenName) {
				User tweet = twitter.createMute(user);
				tweets.add(user);
			} 

		} catch (Exception e) {

			e.printStackTrace();
		}

		return tweets;
	}
	

	/**
	 * use to get friendlist of
	 * 
	 * @param twitter
	 * @return friendListIds,of the user
	 * @throws TwitterException
	 */

	public ArrayList<Map<String, Object>> getFriendsList(Twitter twitter,String screenName,long cursor) throws TwitterException {
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		long[] friendIds;
		IDs friendIDs = null;
		Map<String, Object> tweetInfo = null;
		if (screenName.isEmpty()) {	
			screenName = twitter.getScreenName();		
		}
		try {
			
			Map<String, RateLimitStatus> rateLimitStatusfriend = twitter.getRateLimitStatus("friends");
			RateLimitStatus	followerIdsRateLimit = rateLimitStatusfriend.get("/friends/ids");
			if (followerIdsRateLimit.getRemaining()==0) {
				Thread.sleep(904000);
			}
			friendIDs = twitter.getFriendsIDs(screenName, cursor);
			friendIds = friendIDs.getIDs();
			LinkedList<long[]> chunks = chunks(friendIds, 100);
			Map<String, Object> cursorValue = new HashMap<String, Object>();
			User userInfo =twitter.showUser(screenName);
			cursorValue.put("nextCursor", friendIDs.getNextCursor());
			cursorValue.put("userScreenName", userInfo.getScreenName());
			cursorValue.put("friendsCount",userInfo.getFriendsCount());	
			cursorValue.put("id", userInfo.getId());
			cursorValue.put("userImage", userInfo.getProfileImageURL());
			cursorValue.put("description", userInfo.getDescription());
			cursorValue.put("tweetsCount", userInfo.getStatusesCount());
			cursorValue.put("userLocation", userInfo.getLocation());
			tweets.add(cursorValue);
			RateLimitStatus lookupUserRateLimit = null;
			DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
			Date date = new Date();
			for (int j = 0; j < chunks.size(); j++) {
				
				ResponseList<User> users = twitter.lookupUsers(chunks.get(j));			
				Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("users");
				lookupUserRateLimit = rateLimitStatus.get("/users/lookup");
				Map<String, RateLimitStatus> rateLimitStatusAppi = twitter.getRateLimitStatus("application");
    			RateLimitStatus	AppiRateLimit = rateLimitStatusAppi.get("/application/rate_limit_status");
				
				if ( AppiRateLimit.getRemaining() < 2 || lookupUserRateLimit.getRemaining() < 2) {			
					System.err.println("Reached at rate limit sleeping  for 15 minutes...!");
					Thread.sleep(904000);
				}
				
				for (int i=0;i < users.size() ;i++) {

					tweetInfo =  new HashMap<String, Object>();
					tweetInfo.put("id", users.get(i).getId());
					tweetInfo.put("screenName", users.get(i).getScreenName());
					tweetInfo.put("tweetsCount", users.get(i).getStatusesCount());
					tweetInfo.put("followersCount", users.get(i).getFollowersCount());
					tweetInfo.put("user_image", users.get(i).getProfileImageURL());
					tweetInfo.put("description", users.get(i).getDescription());
					tweetInfo.put("location", users.get(i).getLocation());
					tweetInfo.put("date",dateFormat.format(date));
					tweets.add(tweetInfo);
				}
			}
		} catch (Exception e) {

			e.printStackTrace();
		}

		return tweets;
	}

	/**
	 * use to get followerslist of
	 * 
	 * @param twitter
	 * @return friendListIds,of the user
	 * @throws TwitterException
	 */

	public ArrayList<Map<String, Object>> getFollowersList(Twitter twitter,String screenName,long cursor) throws TwitterException {
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();
		
		if (screenName.isEmpty()) {	
			screenName = twitter.getScreenName();
			}
		
		try {
			IDs followerIDs = null;
			long[] followerIds = null;
			LinkedList<long[]> chunks = null;
			Map<String, RateLimitStatus> rateLimitStatusfollower = twitter.getRateLimitStatus("followers");
			RateLimitStatus	followerIdsRateLimit = rateLimitStatusfollower.get("/followers/ids");
			if (followerIdsRateLimit.getRemaining()==1) {
				Thread.sleep(904000);
				System.err.println("Reached at rate limit sleeping  for 15 minutes...!");
			}
			followerIDs = twitter.getFollowersIDs(screenName, cursor);
			followerIds = followerIDs.getIDs();
			chunks = chunks(followerIds, 100);
			User userInfo = twitter.showUser(screenName);
			Map<String, Object> tweetInfo = null;
			Map<String, Object> cursorValue = new HashMap<String, Object>();
			cursorValue.put("nextCursor", followerIDs.getNextCursor()); 
			cursorValue.put("id", userInfo.getId());
			cursorValue.put("userScreenName", userInfo.getScreenName());
			cursorValue.put("followersCount",userInfo.getFollowersCount());	
			cursorValue.put("userImage", userInfo.getProfileImageURL());
			cursorValue.put("description", userInfo.getDescription());
			cursorValue.put("tweetsCount", userInfo.getStatusesCount());
			cursorValue.put("userLocation", userInfo.getLocation());
			DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
			Date date = new Date();
			tweets.add(cursorValue);
			cursor = followerIDs.getNextCursor();
			RateLimitStatus lookupUserRateLimit = null;
			for (int j = 0; j < chunks.size(); j++) {
				
				// getting user and application limit
				Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("users");
				lookupUserRateLimit = rateLimitStatus.get("/users/lookup");
				Map<String, RateLimitStatus> rateLimitStatusAppi = twitter.getRateLimitStatus("application");
    			RateLimitStatus	AppiRateLimit = rateLimitStatusAppi.get("/application/rate_limit_status");
    			
				//if limit near to zero put the system to sleep so that avoid limit exceeding exception
				if ( AppiRateLimit.getRemaining() < 2 || lookupUserRateLimit.getRemaining() < 2) {			
					System.err.println("Reached at rate limit sleeping  for 15 minutes...!");
					Thread.sleep(904000);
				
				}
				
				ResponseList<User> users = twitter.lookupUsers(chunks.get(j));

				for (int i=0;i<users.size();i++) {	
					tweetInfo = new HashMap<String, Object>();
					tweetInfo.put("id", users.get(i).getId());
					tweetInfo.put("screenName", users.get(i).getScreenName());
					tweetInfo.put("tweetsCount", users.get(i).getStatusesCount());
					tweetInfo.put("followersCount", users.get(i).getFollowersCount());
					tweetInfo.put("user_image", users.get(i).getProfileImageURL());
					tweetInfo.put("description", users.get(i).getDescription());
					tweetInfo.put("location", users.get(i).getLocation());
					tweetInfo.put("date",dateFormat.format(date));
					tweets.add(tweetInfo);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return tweets;
	}

	/**
	 * use to get Ids of followers of
	 * 
	 * @param twitter
	 * @return followetrIds,of the user
	 * @throws TwitterException
	 */

	public ArrayList<String> getFollowerIds(Twitter twitter,String screenName) throws TwitterException {

		ArrayList<String> followersIds = new ArrayList<String>();
			if (screenName.isEmpty()) {
				screenName = twitter.getScreenName();
			}
			
		try {
			long cursor = -1;
			IDs followerIDs = null;
			long[] followerIds = null;
			do {
				
				Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("followers");
				RateLimitStatus	followerIdsRateLimit = rateLimitStatus.get("/followers/ids");
				if (followerIdsRateLimit.getRemaining()==0) {
					Thread.sleep(800000);
				}
				followerIDs = twitter.getFollowersIDs(screenName, cursor);
				followerIds  =  followerIDs.getIDs();
				cursor = followerIDs.getNextCursor();
				for (int i = 0; i < followerIds.length; i++) {
//					String Id =Long.toString(followerIds[i]);
					followersIds.add(Long.toString(followerIds[i]));
				
				}
			} while (cursor !=0);	

		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return followersIds;
		
	}
	/**
	 * use to get Ids of followers of
	 * 
	 * @param twitter
	 * @return followetrIds,of the user
	 * @throws TwitterException
	 */

	public ArrayList<String> getFriendIds(Twitter twitter,String screenName) throws TwitterException {
		
		ArrayList<String> friendsIds = new ArrayList<String>();
		try {
			if (screenName.isEmpty()) {
				screenName = twitter.getScreenName();
			}
			IDs friendIDs = null;
			long[] friendIds = null;
			long cursor = -1;
			do {
				Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("friends");
				RateLimitStatus	followerIdsRateLimit = rateLimitStatus.get("/friends/ids");
				if (followerIdsRateLimit.getRemaining()==0) {
					Thread.sleep(800000);
				}
				friendIDs = twitter.getFriendsIDs(screenName, cursor);
				friendIds = friendIDs.getIDs();
				
				for (int i = 0; i < friendIds.length; i++) {
					String id =Long.toString(friendIds[i]);
					friendsIds.add(id);
				}
				cursor = friendIDs.getNextCursor();
			} while (cursor!=0);
			

		} catch (Exception e) {
			e.printStackTrace();
		}
	
		return friendsIds;
	}


	/**
	 * use to get followerslist of
	 * 
	 * @param twitter
	 * @return friendListIds,of the user
	 * @throws TwitterException
	 */

	public ArrayList<Map<String, Object>> getFollowersInfo(Twitter twitter,LinkedList<long[]> chunks) throws TwitterException {
		ArrayList<Map<String, Object>> tweets = new ArrayList<Map<String, Object>>();

		try {

			for (int j = 0; j < chunks.size(); j++) {

				ResponseList<User> users = twitter.lookupUsers(chunks.get(j));
				Map<String, Object> tweetInfo = null;
				for (User tweet : users) {

					tweetInfo = new HashMap<String, Object>();
					tweetInfo.put("followersCount", tweet.getFollowersCount());
					tweetInfo.put("screenName", tweet.getScreenName().trim());
					tweetInfo.put("id", tweet.getId());
					tweetInfo.put("name", tweet.getName());
					tweetInfo.put("userImage", tweet.getProfileImageURL());
					tweetInfo.put("description", tweet.getDescription());
					tweets.add(tweetInfo);
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	
		return tweets;
	}

	public static LinkedList<long[]> chunks(long[] bigList, int n) {
		int partitionSize = n;
		LinkedList<long[]> partitions = new LinkedList<long[]>();
		for (int i = 0; i < bigList.length; i += partitionSize) {
			long[] bulk = Arrays.copyOfRange(bigList, i,
					Math.min(i + partitionSize, bigList.length));
			partitions.add(bulk);
		}

		return partitions;
	}

	

}
