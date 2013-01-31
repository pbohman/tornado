package com.cs498.team17.shoutout.s3torage;

import com.cs498.team17.shoutout.utils.Configuration;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

public class Shoutout extends StorageObject {
	
	private String message;
	private String user;
	private static final String stgBucketID = Configuration.getInstance().getBucketProperty(Configuration.BUCKET_KEY);
	private static final String stgBasePath = Configuration.getInstance().getBucketProperty(Configuration.SHOUTOUT_PATH_KEY);
	private static final Random randomGenerator = new Random();
	
	public Shoutout(String message, String user) {
		
		this.message = message;
		this.user = user;
		
		super.setBucketName(stgBucketID);
		String ts = new Timestamp(new Date().getTime()).toString();
		String id = Integer.toString(randomGenerator.nextInt()) + message;
		super.setStoragePath(stgBasePath + "/" + user + "/" + ts + "_" + Integer.toString(randomGenerator.nextInt()));
		super.setData(message.getBytes());	
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
	

}
