package minio.services.atomic;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.RemoveBucketArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import io.minio.messages.Bucket;
// --- <<IS-END-IMPORTS>> ---

public final class bucket

{
	// ---( internal utility methods )---

	final static bucket _instance = new bucket();

	static bucket _newInstance() { return new bucket(); }

	static bucket _cast(Object o) { return (bucket)o; }

	// ---( server methods )---




	public static final void addBucket (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(addBucket)>> ---
		// @sigtype java 3.5
		// [i] field:0:required bucketName
		// [o] field:0:required code
		// [o] field:0:required message
		// [o] field:0:required data
		MinioClient client = MinioClient.builder()
				.endpoint("http://localhost:9000")
				.credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
				.build();
		
		IDataCursor pipelineCursor = pipeline.getCursor();
		String bucketName = IDataUtil.getString(pipelineCursor, "bucketName");
		
		String code = "";
		String message = "";
		String data = null;
		
		try {
			client.makeBucket(
					MakeBucketArgs.builder()
					.bucket(bucketName)
					.build()
					);
			
			code = "T";
			message = "success create bucket";
		} catch (InvalidKeyException e) {
			code = "F";
			message = "Invalid Key Exception : " + e.getLocalizedMessage();
		} catch (ErrorResponseException e) {
			code = "F";
			message = "Error Response Exception : " + e.getLocalizedMessage();
		} catch (InsufficientDataException e) {
			code = "F";
			message = "Insufficient Data Exception : " + e.getLocalizedMessage();
		} catch (InternalException e) {
			code = "F";
			message = "Internal Exception : " + e.getLocalizedMessage();
		} catch (InvalidResponseException e) {
			code = "F";
			message = "Invalid Response Exception : " + e.getLocalizedMessage();
		} catch (NoSuchAlgorithmException e) {
			code = "F";
			message = "No Such Algorithm Exception : " + e.getLocalizedMessage();
		} catch (ServerException e) {
			code = "F";
			message = "Server Exception : " + e.getLocalizedMessage();
		} catch (XmlParserException e) {
			code = "F";
			message = "XML Parser Exception : " + e.getLocalizedMessage();
		} catch (IOException e) {
			code = "F";
			message = "IO Exception : " + e.getLocalizedMessage();
		}
		
		IDataUtil.put( pipelineCursor, "code", code );
		IDataUtil.put( pipelineCursor, "message", message );
		IDataUtil.put( pipelineCursor, "data", data );	
		// --- <<IS-END>> ---

                
	}



	public static final void checkBucket (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(checkBucket)>> ---
		// @sigtype java 3.5
		MinioClient client = MinioClient.builder()
				.endpoint("http://localhost:9000")
				.credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
				.build();
		
		IDataCursor pipelineCursor = pipeline.getCursor();
		String bucketName = IDataUtil.getString(pipelineCursor, "bucketName");
		
		String code = "";
		String message = "";
		String data = null;
		
		try {
			boolean found = client.bucketExists(
								BucketExistsArgs.builder()
								.bucket(bucketName)
								.build()
							);
			
			if (found) {
				code = "T";
				message = "bucket exist";
			} else {
				code = "F";
				message = "bucket not exist";
			}
			
		} catch (InvalidKeyException e) {
			code = "F";
			message = "Invalid Key Exception : " + e.getLocalizedMessage();
		} catch (ErrorResponseException e) {
			code = "F";
			message = "Error Response Exception : " + e.getLocalizedMessage();
		} catch (InsufficientDataException e) {
			code = "F";
			message = "Insufficient Data Exception : " + e.getLocalizedMessage();
		} catch (InternalException e) {
			code = "F";
			message = "Internal Exception : " + e.getLocalizedMessage();
		} catch (InvalidResponseException e) {
			code = "F";
			message = "Invalid Response Exception : " + e.getLocalizedMessage();
		} catch (NoSuchAlgorithmException e) {
			code = "F";
			message = "No Such Algorithm Exception : " + e.getLocalizedMessage();
		} catch (ServerException e) {
			code = "F";
			message = "Server Exception : " + e.getLocalizedMessage();
		} catch (XmlParserException e) {
			code = "F";
			message = "XML Parser Exception : " + e.getLocalizedMessage();
		} catch (IOException e) {
			code = "F";
			message = "IO Exception : " + e.getLocalizedMessage();
		}
		
		IDataUtil.put( pipelineCursor, "code", code );
		IDataUtil.put( pipelineCursor, "message", message );
		IDataUtil.put( pipelineCursor, "data", data );	
		// --- <<IS-END>> ---

                
	}



	public static final void deleteBucket (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(deleteBucket)>> ---
		// @sigtype java 3.5
		// [i] field:0:required bucketName
		// [o] field:0:required code
		// [o] field:0:required message
		// [o] field:0:required data
		MinioClient client = MinioClient.builder()
				.endpoint("http://localhost:9000")
				.credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
				.build();
		
		IDataCursor pipelineCursor = pipeline.getCursor();
		String bucketName = IDataUtil.getString(pipelineCursor, "bucketName");
		
		String code = "";
		String message = "";
		String data = null;
		
		try {
			client.removeBucket(
					RemoveBucketArgs.builder()
					.bucket(bucketName)
					.build()
					);
			
			code = "T";
			message = "success delete bucket";
		} catch (InvalidKeyException e) {
			code = "F";
			message = "Invalid Key Exception : " + e.getLocalizedMessage();
		} catch (ErrorResponseException e) {
			code = "F";
			message = "Error Response Exception : " + e.getLocalizedMessage();
		} catch (InsufficientDataException e) {
			code = "F";
			message = "Insufficient Data Exception : " + e.getLocalizedMessage();
		} catch (InternalException e) {
			code = "F";
			message = "Internal Exception : " + e.getLocalizedMessage();
		} catch (InvalidResponseException e) {
			code = "F";
			message = "Invalid Response Exception : " + e.getLocalizedMessage();
		} catch (NoSuchAlgorithmException e) {
			code = "F";
			message = "No Such Algorithm Exception : " + e.getLocalizedMessage();
		} catch (ServerException e) {
			code = "F";
			message = "Server Exception : " + e.getLocalizedMessage();
		} catch (XmlParserException e) {
			code = "F";
			message = "XML Parser Exception : " + e.getLocalizedMessage();
		} catch (IOException e) {
			code = "F";
			message = "IO Exception : " + e.getLocalizedMessage();
		}
		
		IDataUtil.put( pipelineCursor, "code", code );
		IDataUtil.put( pipelineCursor, "message", message );
		IDataUtil.put( pipelineCursor, "data", data );	
		// --- <<IS-END>> ---

                
	}



	public static final void getBuckets (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(getBuckets)>> ---
		// @sigtype java 3.5
		// [o] field:0:required code
		// [o] field:0:required message
		// [o] field:0:required data
		MinioClient client = MinioClient.builder()
				.endpoint("http://localhost:9000")
				.credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
				.build();
		
		String code = "";
		String message = "";
		String data = null;
		
		try {
			List<Bucket> bucketList = client.listBuckets();
			StringBuilder stringBuilder = new StringBuilder();
			
			for (Bucket bucket : bucketList) {
				stringBuilder.append(bucket.name() + " : " + bucket.creationDate());
				stringBuilder.append("\n");
			}
			
			code = "T";
			message = "success";
			data = stringBuilder.toString();
		} catch (InvalidKeyException e) {
			code = "F";
			message = "Invalid Key Exception : " + e.getLocalizedMessage();
		} catch (ErrorResponseException e) {
			code = "F";
			message = "Error Response Exception : " + e.getLocalizedMessage();
		} catch (InsufficientDataException e) {
			code = "F";
			message = "Insufficient Data Exception : " + e.getLocalizedMessage();
		} catch (InternalException e) {
			code = "F";
			message = "Internal Exception : " + e.getLocalizedMessage();
		} catch (InvalidResponseException e) {
			code = "F";
			message = "Invalid Response Exception : " + e.getLocalizedMessage();
		} catch (NoSuchAlgorithmException e) {
			code = "F";
			message = "No Such Algorithm Exception : " + e.getLocalizedMessage();
		} catch (ServerException e) {
			code = "F";
			message = "Server Exception : " + e.getLocalizedMessage();
		} catch (XmlParserException e) {
			code = "F";
			message = "XML Parser Exception : " + e.getLocalizedMessage();
		} catch (IOException e) {
			code = "F";
			message = "IO Exception : " + e.getLocalizedMessage();
		}
		
		IDataCursor pipelineCursor = pipeline.getCursor();
		IDataUtil.put( pipelineCursor, "code", code );
		IDataUtil.put( pipelineCursor, "message", message );
		IDataUtil.put( pipelineCursor, "data", data );
		// --- <<IS-END>> ---

                
	}
}

