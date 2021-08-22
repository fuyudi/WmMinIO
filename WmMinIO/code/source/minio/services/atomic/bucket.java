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
import io.minio.MinioClient;
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




	public static final void getBuckets (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(getBuckets)>> ---
		// @sigtype java 3.5
		// [o] field:0:required data
		MinioClient client = MinioClient.builder()
				.endpoint("http://localhost:9000")
				.credentials("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
				.build();
		
		String data = "";
		
		try {
			List<Bucket> bucketList = client.listBuckets();
			StringBuilder stringBuilder = new StringBuilder();
			
			for (Bucket bucket : bucketList) {
				stringBuilder.append(bucket.name() + " : " + bucket.creationDate());
				stringBuilder.append("\n");
			}
			
			data = stringBuilder.toString();
		} catch (InvalidKeyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ErrorResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InsufficientDataException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InternalException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ServerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XmlParserException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		IDataCursor pipelineCursor = pipeline.getCursor();
		IDataUtil.put( pipelineCursor, "data", data );
		// --- <<IS-END>> ---

                
	}
}

