package org.onsurity;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomListener {

    private static final int MAX_RETRIES = 14;
    private static final int BASE_TIMEOUT = 20;
    private final String sqsUrl;
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String awsRegion;
    private AmazonSQS amazonSQS;
    private final ObjectMapper mapper = new ObjectMapper();

    public CustomListener(String sqsUrl, String awsAccessKey, String awsSecretKey, String awsRegion) {
        this.sqsUrl = sqsUrl;
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.awsRegion = awsRegion;
        initializeSQSClient();
    }

    private void initializeSQSClient() {
        AWSCredentialsProvider awsCredentialsProvider = new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            }

            @Override
            public void refresh() {}
        };

        this.amazonSQS = AmazonSQSClientBuilder.standard()
                .withRegion(awsRegion)
                .withCredentials(awsCredentialsProvider)
                .build();
    }

    private void handleRetry(int receiveCount, String sqsUrl, Message message) {
        if (receiveCount <= MAX_RETRIES) {
            int delay = calculateExponentialBackoff(receiveCount);
            this.amazonSQS.changeMessageVisibility(new ChangeMessageVisibilityRequest()
                    .withQueueUrl(sqsUrl)
                    .withReceiptHandle(message.getReceiptHandle())
                    .withVisibilityTimeout(delay));
        } else {
            System.err.println("Max retries reached; moving message to dead-letter queue");
        }
    }

    private int calculateExponentialBackoff(int retryAttempt) {
        return retryAttempt * BASE_TIMEOUT + BASE_TIMEOUT;
    }

    public List<JsonNode> readMessages() {
        List<JsonNode> jsonNodes = new ArrayList<>();
        boolean keepPolling = true;

        while (keepPolling) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl)
                    .withMaxNumberOfMessages(10)
                    .withWaitTimeSeconds(20)
                    .withAttributeNames("ApproximateReceiveCount"); // Retrieve all message attributes

            ReceiveMessageResult receiveMessageResult = this.amazonSQS.receiveMessage(receiveMessageRequest);
            List<Message> messages = receiveMessageResult.getMessages();



            if (messages.isEmpty()) {
                keepPolling = false;
            }

            for (Message message : messages) {
                try {
                    JsonNode sqsMessage = mapper.readTree(message.getBody());
                    jsonNodes.add(sqsMessage);
                    amazonSQS.deleteMessage(sqsUrl, message.getReceiptHandle());
                } catch (IOException e) {
                    int receiveCount= Integer.parseInt(message.getAttributes().get("ApproximateReceiveCount"));
                    handleRetry(receiveCount, sqsUrl, message);
                }
            }
        }
        return jsonNodes;
    }
}