package com.cs498.team17.shoutout.s3torage;

import com.cs498.team17.shoutout.utils.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Random;
import java.util.Date;
import java.sql.Timestamp;

public class Shoutout extends StorageObject implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	private String message;
	private String user;
	private static final String stgBucketID = Configuration.getInstance().getBucketProperty(Configuration.BUCKET_KEY);
	private static final String stgBasePath = Configuration.getInstance().getBucketProperty(Configuration.SHOUTOUT_PATH_KEY);
	private static final Random randomGenerator = new Random();

	public Shoutout(String message, String user) {
		
		this.message = message;
		this.user = user;
		
		super.setBucketName(stgBucketID);
		
		java.util.Date date= new java.util.Date();
		Timestamp ts = new Timestamp(date.getTime());
		super.setStoragePath(ts + Integer.toString(randomGenerator.nextInt()) );
	}
	
	public static List<Shoutout> fromS3(S3StorageManager s3Manager) throws IOException, ClassNotFoundException{
		List<Shoutout> shoutouts = new ArrayList<Shoutout>();
		Iterator<InputStream> streams = s3Manager.getObjects(stgBucketID);
		while(streams.hasNext()){
			InputStream stream = streams.next();
			ObjectInputStream in = new ObjectInputStream(stream);
			shoutouts.add((Shoutout)in.readObject());
		}
		return shoutouts;
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
