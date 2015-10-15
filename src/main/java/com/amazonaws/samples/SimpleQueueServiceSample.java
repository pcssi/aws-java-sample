package com.amazonaws.samples;

/*
 * Copyright 2010-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map.Entry;

/**
 * This sample demonstrates how to make basic requests to Amazon SQS using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQS. For more
 * information on Amazon SQS, see http://aws.amazon.com/sqs.
 * <p>
 * Fill in your AWS access credentials in the provided credentials file
 * template, and be sure to move the file to the default location
 * (~/.aws/credentials) where the sample code will load the credentials from.
 * <p>
 * <b>WARNING:</b> To avoid accidental leakage of your credentials, DO NOT keep
 * the credentials file in your source directory.
 */
public class SimpleQueueServiceSample {

  /**
   * TODO IoC
   */
  private static final Regions REGION = Regions.US_EAST_1;

  /**
   * TODO IoC
   */
  private static final String PROFILE_NAME = "foo";

  private static final Logger LOG = LoggerFactory.getLogger(SimpleQueueServiceSample.class);

  public static void main(String[] args) throws Exception {
    final AmazonSQS sqs = getAmazonSQS();


    LOG.trace("===========================================");
    LOG.trace("Getting Started with Amazon SQS");
    LOG.trace("===========================================\n");

    try {
      // Create a queue
      LOG.trace("Creating a new SQS queue called MyQueue.\n");
      final CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
      String myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

      // List queues
      LOG.trace("Listing all queues in your account.\n");
      for (String queueUrl : sqs.listQueues().getQueueUrls()) {
        LOG.trace("  QueueUrl: " + queueUrl);
      }
      LOG.trace("\n");

      // Send a message
      LOG.trace("Sending a message to MyQueue.\n");
      final SendMessageRequest sendMessageRequest = new SendMessageRequest(myQueueUrl, "This is my message text.");
      sqs.sendMessage(sendMessageRequest);

      // Receive messages
      LOG.trace("Receiving messages from MyQueue.\n");
      final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl);
      final List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
      for (Message message : messages) {
        LOG.trace("  Message");
        LOG.trace("    MessageId:     " + message.getMessageId());
        LOG.trace("    ReceiptHandle: " + message.getReceiptHandle());
        LOG.trace("    MD5OfBody:     " + message.getMD5OfBody());
        LOG.trace("    Body:          " + message.getBody());
        for (Entry<String, String> entry : message.getAttributes().entrySet()) {
          LOG.trace("  Attribute");
          LOG.trace("    Name:  " + entry.getKey());
          LOG.trace("    Value: " + entry.getValue());
        }
      }
      LOG.trace("\n");

      // Delete a message
      LOG.trace("Deleting a message.\n");
      String messageRecieptHandle = messages.get(0).getReceiptHandle();
      final DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(myQueueUrl, messageRecieptHandle);
      sqs.deleteMessage(deleteMessageRequest);

      // Delete a queue
      LOG.trace("Deleting the test queue.\n");
      final DeleteQueueRequest deleteQueueRequest = new DeleteQueueRequest(myQueueUrl);
      sqs.deleteQueue(deleteQueueRequest);
    } catch (AmazonServiceException ase) {
      LOG.error("Caught an AmazonServiceException, which means your request made it " +
          "to Amazon SQS, but was rejected with an error response for some reason.");
      LOG.error("Error Message:    " + ase.getMessage());
      LOG.error("HTTP Status Code: " + ase.getStatusCode());
      LOG.error("AWS Error Code:   " + ase.getErrorCode());
      LOG.error("Error Type:       " + ase.getErrorType());
      LOG.error("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
      LOG.error("Caught an AmazonClientException, which means the client encountered " +
          "a serious internal problem while trying to communicate with SQS, such as not " +
          "being able to access the network.");
      LOG.error("Error Message: " + ace.getMessage());
    }
  }

  private static AmazonSQS getAmazonSQS() {
    return getAmazonSQS(Region.getRegion(REGION));
  }

  private static AmazonSQS getAmazonSQS(final Region region) {
    /**
     * TODO IoC
     */
    final AWSCredentialsProvider credentialsProvider = new ProfileCredentialsProvider(PROFILE_NAME);
    final AmazonSQS sqs = new AmazonSQSClient(credentialsProvider);
    sqs.setRegion(region);
    return sqs;
  }
}