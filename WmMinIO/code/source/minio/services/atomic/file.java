package minio.services.atomic;

// -----( IS Java Code Template v1.2

import com.wm.data.*;
import com.wm.util.Values;
import com.wm.app.b2b.server.Service;
import com.wm.app.b2b.server.ServiceException;
// --- <<IS-START-IMPORTS>> ---
import io.minio.GetObjectArgs;
import io.minio.MinioClient;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
// --- <<IS-END-IMPORTS>> ---

public final class file

{
	// ---( internal utility methods )---

	final static file _instance = new file();

	static file _newInstance() { return new file(); }

	static file _cast(Object o) { return (file)o; }

	// ---( server methods )---




	public static final void getFile (IData pipeline)
        throws ServiceException
	{
		// --- <<IS-START(getFile)>> ---
		// @sigtype java 3.5
		// [i] field:0:required bucketName
		// [i] field:0:required filename
		// [o] field:0:required code
		// [o] field:0:required message
		// [o] field:0:required data
		IDataCursor pipelineCursor = pipeline.getCursor();
		String bucketName = IDataUtil.getString(pipelineCursor, "bucketName");
		String filename = IDataUtil.getString(pipelineCursor, "filename");
		
		String code = "";
		String message = "";
		String data = null;
		
		IData inCfg = IDataFactory.create();
		IDataCursor typeCfg = inCfg.getCursor();
		//		IDataUtil.put(typeCfg, "type", "gv"); //for gv configuration
		IDataUtil.put(typeCfg, "type", "");
		
		IData outCfg = IDataFactory.create();	
		
		try {
			outCfg = Service.doInvoke("minio.services.atomic.config", "getConfig", inCfg);
			IDataCursor outputCfg = outCfg.getCursor();
			String endpoint = IDataUtil.getString(outputCfg, "endpoint");
			String accessKey = IDataUtil.getString(outputCfg, "accessKey");
			String secretKey = IDataUtil.getString(outputCfg, "secretKey");
			
			typeCfg.destroy();
			outputCfg.destroy();
			
			MinioClient client = MinioClient.builder()
					.endpoint(endpoint)
					.credentials(accessKey, secretKey)
					.build();
			
			InputStream obj = client.getObject(GetObjectArgs.builder()
					.bucket(bucketName)
					.object(filename)
					.build());
			
			byte[] buf = new byte[16384];
		    int bytesRead;
		    StringBuilder stringBuilder = new StringBuilder();
		    while ((bytesRead = obj.read(buf, 0, buf.length)) >= 0) {
		    	stringBuilder.append(new String(buf, 0, bytesRead, StandardCharsets.UTF_8));
		    }
		    data = stringBuilder.toString();
			
			code = "T";
			message = "success get file";
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
		} catch (Exception e) {
			code = "F";
			message = "Exception : " + e.getLocalizedMessage();
		}
		
		IDataUtil.put( pipelineCursor, "code", code );
		IDataUtil.put( pipelineCursor, "message", message );
		IDataUtil.put( pipelineCursor, "data", data );
		// --- <<IS-END>> ---

                
	}
}

