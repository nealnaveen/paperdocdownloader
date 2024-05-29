package com.ptab;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.core.sync.RequestBody;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PaperDocDownloader {

    private static final String BUCKET_NAME = "paperdocuments";
    private static final String TABLE_NAME = "datacollector";
    private static final String CONFIG_NAME = "PaperDocConfig";
    private static final String BASE_URL = "https://developer.uspto.gov/ptab-api/documents";

    private final DynamoDbClient dynamoDbClient;
    private final S3Client s3Client;
    private final ObjectMapper objectMapper;

    public PaperDocDownloader() {
        dynamoDbClient = DynamoDbClient.builder().region(Region.US_EAST_1).build();
        s3Client = S3Client.builder().region(Region.US_EAST_1).build();
        objectMapper = new ObjectMapper();
    }

    public Map<String, Integer> getConfigFromDynamoDb() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ConfigName", AttributeValue.builder().s(CONFIG_NAME).build());

        GetItemRequest request = GetItemRequest.builder().tableName(TABLE_NAME).key(key).build();
        GetItemResponse response = dynamoDbClient.getItem(request);

        int startNumber = Integer.parseInt(response.item().get("StartNumber").n());
        int totalQuantity = Integer.parseInt(response.item().get("TotalQuantity").n());

        Map<String, Integer> config = new HashMap<>();
        config.put("recordStartNumber", startNumber);
        config.put("recordTotalQuantity", totalQuantity);
        return config;
    }

    public void updateConfigInDynamoDb(int recordStartNumber, int recordTotalQuantity) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ConfigName", AttributeValue.builder().s(CONFIG_NAME).build());
        item.put("StartNumber", AttributeValue.builder().n(String.valueOf(recordStartNumber)).build());
        item.put("TotalQuantity", AttributeValue.builder().n(String.valueOf(recordTotalQuantity)).build());

        PutItemRequest request = PutItemRequest.builder().tableName(TABLE_NAME).item(item).build();
        dynamoDbClient.putItem(request);
    }

    public JsonNode getDocuments(int recordStartNumber, int recordTotalQuantity) throws IOException {
        String url = String.format("%s?recordStartNumber=%d&recordTotalQuantity=%d", BASE_URL, recordStartNumber, recordTotalQuantity);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(url);
        CloseableHttpResponse response = httpClient.execute(request);

        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(response.getEntity().getContent());
    }

    public byte[] downloadDocument(String documentNumber) throws IOException {
        String url = String.format("%s/%s/download", BASE_URL, documentNumber);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(url);
        CloseableHttpResponse response = httpClient.execute(request);
        return response.getEntity().getContent().readAllBytes();
    }

    public void uploadToS3(String proceedingNumber, String documentName, byte[] fileContent) {
        System.out.println(documentName);
        String key = String.format("%s/%s", proceedingNumber, documentName);
        putS3Object( BUCKET_NAME, key, fileContent);
        System.out.println("Uploaded " + key + " to S3 bucket " + BUCKET_NAME);
    }

    public  void putS3Object(String bucketName, String objectKey, byte[] fileContent) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(fileContent));
            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);

        } catch (S3Exception e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public void processDocuments() {
        try {
            Map<String, Integer> config = getConfigFromDynamoDb();
            int recordStartNumber = config.get("recordStartNumber");
            int recordTotalQuantity = config.get("recordTotalQuantity");

            while (true) {
                JsonNode responseJson = getDocuments(recordStartNumber, recordTotalQuantity);
                JsonNode documents = responseJson.path("results");

                if (documents.isEmpty()) {
                    break;
                }

                if(recordStartNumber >=100) {
                    break;
                }

                for (JsonNode document : documents) {
                    if (document.path("documentCategory").asText().equals("Exhibit")) {
                        continue;
                    }

                    String proceedingNumber = document.path("proceedingNumber").asText("unknown");
                    String documentNumber = document.path("documentNumber").asText();
                    String documentName = document.path("documentName").asText();
                    try {
                        byte[] fileContent = downloadDocument(documentNumber);
                        uploadToS3(proceedingNumber, documentName, fileContent);
                    } catch (Exception e) {
                        System.err.println("Failed to process document " + documentNumber + ": " + e.getMessage());
                    }
                }

                recordStartNumber += recordTotalQuantity;
                updateConfigInDynamoDb(recordStartNumber, recordTotalQuantity);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PaperDocDownloader processor = new PaperDocDownloader();
        processor.processDocuments();
    }
}
