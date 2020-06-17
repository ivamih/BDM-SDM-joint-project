import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;

public class DataLake {
    public  static String resources = "C:\\Users\\Iva\\Desktop\\UPC\\BDM\\Project\\joint-project\\src\\main\\resources\\";
    Regions clientRegion;
    String bucketName;
    DataLake(Regions clientRegion, String bucketName)
    {
        this.clientRegion = clientRegion;
        this.bucketName = bucketName;
    }

    public void getFileFromS3 (String file) throws IOException {
        String key = file;

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(this.clientRegion)
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();

            System.out.println("Downloading an object");
            fullObject = s3Client.getObject(new GetObjectRequest(this.bucketName, key));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
            System.out.println("Content: ");
            displayTextInputStreamAndSaveFile(fullObject.getObjectContent(), file);

            GetObjectRequest rangeObjectRequest = new GetObjectRequest(this.bucketName, key)
                    .withRange(0, 9);
            objectPortion = s3Client.getObject(rangeObjectRequest);
//            System.out.println("Printing bytes retrieved.");
//            displayTextInputStreamAndSaveFile(objectPortion.getObjectContent(), file);

            ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename=example.txt");
            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(this.bucketName, key)
                    .withResponseHeaders(headerOverrides);
            headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);


//            displayTextInputStreamAndSaveFile(headerOverrideObject.getObjectContent(), file);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException | IOException e) {
            e.printStackTrace();
        } finally {
            if (fullObject != null) {
                fullObject.close();
            }
            if (objectPortion != null) {
                objectPortion.close();
            }
            if (headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }
    }

    private static void displayTextInputStreamAndSaveFile(InputStream input, String filename) throws IOException {
        InputStream reader = new BufferedInputStream(input);
        File file = new File(filename);
        OutputStream writer = new BufferedOutputStream(new FileOutputStream(file));
        int read = -1;

        while ((read = reader.read()) != -1) {
//            System.out.println(read);
            writer.write(read);

        }


        writer.flush();
        writer.close();
        reader.close();
    }

    public void uploadFileToS3(String file)
    {
          String stringObjKeyName = file;
        String fileObjKeyName = file;
        String fileName = resources+file;
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(this.clientRegion)
                    .build();

            s3Client.putObject(this.bucketName, stringObjKeyName, "Uploaded String Object");

            InputStream is = new FileInputStream(fileName);
            PutObjectRequest request = new PutObjectRequest(this.bucketName, fileObjKeyName,is,new ObjectMetadata());
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("title", "someTitle");
            request.setMetadata(metadata);
            s3Client.putObject(request);
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        DataLake u = new DataLake(Regions.EU_WEST_1, "joint-project" );
//        UPLOAD
        u.uploadFileToS3("Busers.csv");
        u.uploadFileToS3("Susers.csv");
        u.uploadFileToS3("belgrade_graph.json");
        u.uploadFileToS3("skopje_graph.json");
        u.uploadFileToS3("skopje_paths_no_time.csv");
        u.uploadFileToS3("belgrade_paths_no_time.csv");
//       DOWNLOAD
        u.getFileFromS3("Busers.csv");
    }
}

