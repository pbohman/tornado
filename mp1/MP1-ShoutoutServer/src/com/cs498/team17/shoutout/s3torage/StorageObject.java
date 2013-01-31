/*
 * Copyright 2010-2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.cs498.team17.shoutout.s3torage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * This is a general purpose object used for storing data on S3.
 * The mimeType and data variables need only be set if you are
 * planning to initiate a storage call.  If you do not specify a
 * mimeType "text/html" is the default.
 */
public abstract class StorageObject {

	private String bucketName;
	private String storagePath;
	private String mimeType="application/octet-stream ";

	public void setBucketName(String bucketName) {
		/**
		 * S3 prefers that the bucket name be lower case.  While you can
		 * create buckets with different cases, it will error out when
		 * being passed through the AWS SDK due to stricter checking.
		 */
		this.bucketName = bucketName.toLowerCase();
		//this.bucketName = bucketName;
	}
	

	public String getBucketName() {
		return bucketName;
	}

	public byte[] getData() {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(this);
			return bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}

	public String getStoragePath() {
		return storagePath;
	}

	public void setStoragePath(String storagePath) {
		this.storagePath = storagePath;
	}

	/**
	 * Convenience method to construct the URL that points to
	 * an object stored on S3 based on the bucket name and
	 * storage path.
	 * @return the S3 URL for the object
	 */
    public String getAwsUrl() {
        return new S3StorageManager().getResourceUrl(bucketName, storagePath);
    }

	public String getMimeType() {
		return mimeType;
	}

	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

}
