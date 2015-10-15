/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package com.amazonaws.samples;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This sample demonstrates how to make basic requests to Amazon S3 using
 * the AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web Services developer
 * account, and be signed up to use Amazon S3. For more information on
 * Amazon S3, see http://aws.amazon.com/s3.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in
 * ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows
 * users) before you try to run this sample.
 */
class S3Sample {

  private static final Logger LOG = LoggerFactory.getLogger(S3Sample.class);

  /**
   * TODO IoC
   */
  private static final Regions REGION = Regions.US_EAST_1;

  /**
   * TODO IoC
   */
  private static final String PROFILE_NAME = "foo";

  public static void main(final String[] args) throws IOException {
    executeS3Example();
  }

  @SuppressWarnings("unused")
  private static void testAllRegions() throws IOException {
    final List<Regions> regionsForTesting = getRegionsForTesting();
    for (Regions region : regionsForTesting) {
      final Region regionFromRegionsEnum = Region.getRegion(region);
      LOG.warn("{}\n", regionFromRegionsEnum);
      executeS3Example(regionFromRegionsEnum);
    }
  }

  /**
   * After testing Regions.CN_NORTH_1 and Regions.GovCloud did not contain credentials.
   */
  private static List<Regions> getRegionsForTesting() {
    final Regions[] regions = Regions.values();
    final List<Regions> regionsList = new ArrayList<>(Arrays.asList(regions));
    regionsList.remove(Regions.CN_NORTH_1);
    regionsList.remove(Regions.GovCloud);
    ((ArrayList) regionsList).trimToSize();
    return Collections.unmodifiableList(regionsList);
  }

  private static void executeS3Example() throws IOException {
    executeS3Example(Region.getRegion(REGION));
  }

  private static void executeS3Example(final Region region) throws IOException {
    //TODO make aspect
    final long startTime = System.nanoTime();
    final AmazonS3 s3 = getAmazonS3(region);

    LOG.info("===========================================");
    LOG.info("Getting Started with Amazon S3");
    LOG.info("===========================================\n");

    try {
            /*
             * Create a new S3 bucket - Amazon S3 bucket names are globally unique,
             * so once a bucket name has been taken by any user, you can't create
             * another bucket with that same name.
             *
             * You can optionally specify a location for your bucket if you want to
             * keep your data closer to your applications or users.
             */
      String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();
      String key = "MyObjectKey";
      LOG.info("Creating bucket {}\n", bucketName);
      s3.createBucket(bucketName);

            /*
             * List the buckets in your account
             */
      LOG.info("Listing buckets");
      for (Bucket bucket : s3.listBuckets()) {
        LOG.info(" - {}", bucket.getName());
      }
      LOG.info("\n");

            /*
             * Upload an object to your bucket - You can easily upload a file to
             * S3, or upload directly an InputStream if you know the length of
             * the data in the stream. You can also specify your own metadata
             * when uploading to S3, which allows you set a variety of options
             * like content-type and content-encoding, plus additional metadata
             * specific to your applications.
             */
      LOG.info("Uploading a new object to S3 from a file\n");
      final PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, createSampleFile());
      s3.putObject(putObjectRequest);

            /*
             * Download an object - When you download an object, you get all of
             * the object's metadata and a stream from which to read the contents.
             * It's important to read the contents of the stream as quickly as
             * possibly since the data is streamed directly from Amazon S3 and your
             * network connection will remain open until you read all the data or
             * close the input stream.
             *
             * GetObjectRequest also supports several other options, including
             * conditional downloading of objects based on modification times,
             * ETags, and selectively downloading a range of an object.
             */
      LOG.info("Downloading an object");
      final GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, key);
      final S3Object object = s3.getObject(getObjectRequest);
      LOG.info("Content-Type: {}", object.getObjectMetadata().getContentType());
      try (final InputStream objectContent = object.getObjectContent()) {
        displayTextInputStream(objectContent);
      }

            /*
             * List objects in your bucket by prefix - There are many options for
             * listing the objects in your bucket.  Keep in mind that buckets with
             * many objects might truncate their results when listing their objects,
             * so be sure to check if the returned object listing is truncated, and
             * use the AmazonS3.listNextBatchOfObjects(...) operation to retrieve
             * additional results.
             */
      LOG.info("Listing objects");
      final ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucketName)
          .withPrefix("My");
      final ObjectListing objectListing = s3.listObjects(listObjectsRequest);
      for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        LOG.info(" - {}", objectSummary.getKey() + "  " +
            "(size = {}", objectSummary.getSize() + ")");
      }
      LOG.info("\n");

            /*
             * Delete an object - Unless versioning has been turned on for your bucket,
             * there is no way to undelete an object, so use caution when deleting objects.
             */
      LOG.info("Deleting an object\n");
      s3.deleteObject(bucketName, key);

            /*
             * Delete a bucket - A bucket must be completely empty before it can be
             * deleted, so remember to delete any objects from your buckets before
             * you try to delete them.
             */
      LOG.info("Deleting bucket " + bucketName + "\n");
      s3.deleteBucket(bucketName);
    } catch (AmazonServiceException ase) {
      LOG.info("Caught an AmazonServiceException, which means your request made it "
          + "to Amazon S3, but was rejected with an error response for some reason.");
      LOG.info("Error Message:    {}", ase.getMessage());
      LOG.info("HTTP Status Code: {}", ase.getStatusCode());
      LOG.info("AWS Error Code:   {}", ase.getErrorCode());
      LOG.info("Error Type:       {}", ase.getErrorType());
      LOG.info("Request ID:       {}", ase.getRequestId());
    } catch (AmazonClientException ace) {
      LOG.info("Caught an AmazonClientException, which means the client encountered "
          + "a serious internal problem while trying to communicate with S3, "
          + "such as not being able to access the network.");
      LOG.info("Error Message: {}", ace.getMessage());
    }
    LOG.info("\n");
    final long endTime = System.nanoTime();
    final long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds. See: http://stackoverflow.com/a/180191
    LOG.warn("Duration in nanoseconds: {}", duration);
    LOG.warn("Duration in milliseconds: {}", TimeUnit.MILLISECONDS.convert(duration, TimeUnit.NANOSECONDS));
    LOG.warn("Duration in seconds: {}", TimeUnit.SECONDS.convert(duration, TimeUnit.NANOSECONDS));
    LOG.warn("High Precision Duration in nanoseconds: {}", duration);
    LOG.warn("High Precision Duration in milliseconds: {}", (double) duration / 1000000.0d);
    LOG.warn("High Precision Duration in seconds: {}", (double) duration / 1000000000.0d);
    LOG.warn("\n");
  }

  /**
   * Create your credentials file at ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
   * and save the following lines after replacing the underlined values with your own.
   * <p>
   * [default]
   * aws_access_key_id = YOUR_ACCESS_KEY_ID
   * aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
   */
  @SuppressWarnings("unused")
  private static AmazonS3 getAmazonS3() {
    return getAmazonS3(Region.getRegion(REGION));
  }

  /**
   * Create your credentials file at ~/.aws/credentials (C:\Users\USER_NAME\.aws\credentials for Windows users)
   * and save the following lines after replacing the underlined values with your own.
   * <p>
   * [default]
   * aws_access_key_id = YOUR_ACCESS_KEY_ID
   * aws_secret_access_key = YOUR_SECRET_ACCESS_KEY
   */
  private static AmazonS3 getAmazonS3(final Region region) {
    /**
     * TODO IoC
     */
    final AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(PROFILE_NAME);
    final AmazonS3 s3 = new AmazonS3Client(credentialsProvider);
    s3.setRegion(region);
    return s3;
  }

  /**
   * Creates a temporary file with text data to demonstrate uploading a file
   * to Amazon S3
   *
   * @return A newly created temporary file with text data.
   * @throws IOException
   */
  private static File createSampleFile() throws IOException {
    final File file = File.createTempFile("aws-java-sdk-", ".txt");
    file.deleteOnExit();
    try (final OutputStream outputStream = new FileOutputStream(file);
         final Writer writer = new OutputStreamWriter(outputStream)) {
      writer.write("abcdefghijklmnopqrstuvwxyz\n");
      writer.write("01234567890112345678901234\n");
      writer.write("!@#$%^&*()-=[]{};':',.<>/?\n");
      writer.write("01234567890112345678901234\n");
      writer.write("abcdefghijklmnopqrstuvwxyz\n");
    }
    return file;
  }

  /**
   * Displays the contents of the specified input stream as text.
   *
   * @param input The input stream to display as text.
   * @throws IOException
   */
  private static void displayTextInputStream(final InputStream input) throws IOException {
    try (final Reader streamReader = new InputStreamReader(input);
         final BufferedReader reader = new BufferedReader(streamReader)) {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        LOG.info("    {}", line);
      }
      LOG.info("\n");
    }
  }
}
