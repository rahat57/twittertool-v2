package org.xululabs.twittertool_v2;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;


public class Main {

	public static void main(String[] args) {
		

		System.err.println("in main ");
			Vertx vertx = Vertx.vertx();
			vertx.deployVerticle(new DeployServer());

		
//		vertx.deployVerticle(new SearchServer());
//		vertx.deployVerticle(new SearchUserServer());
//		vertx.deployVerticle(new SearchUserRelationServer());
//		vertx.deployVerticle(new SearchUserInfluenceServer());
//		vertx.deployVerticle(new UserInfoServer());
//		vertx.deployVerticle(new IndexTweetsServer());
//		vertx.deployVerticle(new IndexUserServer());
//		vertx.deployVerticle(new IndexUserInfluenceServer());
//		vertx.deployVerticle(new RetweetServer());
//		vertx.deployVerticle(new FollowUserServer());
//		vertx.deployVerticle(new UnfollowServer());
//		vertx.deployVerticle(new MuteServer());
//		vertx.deployVerticle(new BlockServer());
		
		
	}

}
