package com.withjoy;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
/**
 *
 * @author akiramadono
 */
public class S3Reader {

  public static final int BUFFER_SIZE = 64 * 1024;

  private AmazonS3 s3;

  public S3Reader(String accessKey, String secretKey) {
      
      BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey); 
      s3 = AmazonS3ClientBuilder.standard()
              .withRegion(Regions.US_WEST_1)
              .withCredentials(new AWSStaticCredentialsProvider(creds)).build();
  }
  
  public void getListObjectKeys(String bucketName, List<String> files){
      final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(1);
            ListObjectsV2Result result;
            do {               
               result = s3.listObjectsV2(req);               
               for (S3ObjectSummary objectSummary : 
                   result.getObjectSummaries()) {
                        files.add(objectSummary.getKey());
                   }
               req.setContinuationToken(result.getNextContinuationToken());
            } while(result.isTruncated() == true ); 
  }

  /**
   * Read a character oriented file from S3
   *
   * @param bucketName  Name of bucket
   * @param key         File Name
   * @throws IOException
   * @return BufferedReader
   */
  public BufferedReader readFromS3(String bucketName, String key) throws IOException {
    S3Object s3object = s3.getObject(new GetObjectRequest(
            bucketName, key));
    System.out.println(s3object.getObjectMetadata().getContentType());
    System.out.println(s3object.getObjectMetadata().getContentLength());
    return new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
  }
}
