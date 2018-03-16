package org.apache.nifi.BlobUpload;

/**
 * Hello world!
 *
 */
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;
//import com.sun.xml.internal.bind.v2.TODO;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.annotate.Annotate;
import in.dream_lab.bm.stream_iot.tasks.io.AzureBlobUploadTask;
import in.dream_lab.bm.stream_iot.tasks.predict.DecisionTreeTrainBatched;
import net.minidev.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;



@Tags({ "SenML" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class BlobUploadProcessor extends AbstractProcessor {

	public static final PropertyDescriptor IO_AZURE_BLOB_UPLOAD_DIR_NAME = new PropertyDescriptor.Builder()
			.name("BlobUploadDirectoryName").displayName("BlobUploadDirectoryName").description("Blob upload directory name")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor TRAIN_DATASET_NAME = new PropertyDescriptor.Builder()
			.name("TrainDatasetName").displayName("TrainDatasetName").description("Training data set name")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor IO_AZURE_BLOB_USE_MSG_FIELD = new PropertyDescriptor.Builder()
			.name("AzureBlobUseMsgField").displayName("AzureBlobUseMsgField").description("Azure blob use msg field")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor IO_AZURE_STORAGE_CONN_STR = new PropertyDescriptor.Builder()
			.name("StorageContainerString").displayName("StorageContainerString").description("Container Storage string")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor IO_AZURE_BLOB_CONTAINER_NAME = new PropertyDescriptor.Builder()
			.name("AzureContainerName").displayName("AzureContainerName").description("Blob Container name in Azure")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor IO_AZURE_BLOB_UPLOAD_FILE_SOURCE_PATH = new PropertyDescriptor.Builder()
			.name("UploadFileSourcePath").displayName("UploadFileSourcePath").description("Path to the source file to upload")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	private Properties p;
	private static Logger l;

	public static void initLogger(Logger l_) {
		l = l_;
	}
	
	AzureBlobUploadTask azureBlobUploadTask;
    String baseDirname="";
    String fileName="T";
    String datasetName="";


	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(IO_AZURE_BLOB_UPLOAD_DIR_NAME);
		descriptors.add(TRAIN_DATASET_NAME);
		descriptors.add(IO_AZURE_BLOB_USE_MSG_FIELD);
		descriptors.add(IO_AZURE_STORAGE_CONN_STR);
		descriptors.add(IO_AZURE_BLOB_CONTAINER_NAME);
		descriptors.add(IO_AZURE_BLOB_UPLOAD_FILE_SOURCE_PATH);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(success);
		this.relationships = Collections.unmodifiableSet(relationships);

	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		try {
			initLogger(LoggerFactory.getLogger("APP"));
			p = new Properties();
			p.setProperty("IO.AZURE_BLOB_UPLOAD.DIR_NAME", 
					context.getProperty(IO_AZURE_BLOB_UPLOAD_DIR_NAME).getValue().toString());
			p.setProperty("TRAIN.DATASET_NAME", 
					context.getProperty(TRAIN_DATASET_NAME).getValue().toString());
			p.setProperty("IO.AZURE_BLOB.USE_MSG_FIELD", 
					context.getProperty(IO_AZURE_BLOB_USE_MSG_FIELD).getValue().toString());
			p.setProperty("IO.AZURE_STORAGE_CONN_STR", 
					context.getProperty(IO_AZURE_STORAGE_CONN_STR).getValue().toString());
			p.setProperty("IO.AZURE_BLOB.CONTAINER_NAME", 
					context.getProperty(IO_AZURE_BLOB_CONTAINER_NAME).getValue().toString());
			p.setProperty("IO.AZURE_BLOB_UPLOAD.FILE_SOURCE_PATH", 
					context.getProperty(IO_AZURE_BLOB_UPLOAD_FILE_SOURCE_PATH).getValue().toString());
			
			azureBlobUploadTask=new AzureBlobUploadTask();
	        //ToDO:  unique file path for every thread in local before uploading

	        baseDirname=p.getProperty("IO.AZURE_BLOB_UPLOAD.DIR_NAME").toString();
	        datasetName=p.getProperty("TRAIN.DATASET_NAME").toString();
	        
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final HashMap<String, String> hm = new HashMap<String, String>();
		final JSONObject jobj = new JSONObject();
		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		
		String res = "0";
		
		session.read(flowFile, new InputStreamCallback() {

			public void process(InputStream arg0) throws IOException {
				String json = IOUtils.toString(arg0, "UTF-8");
				String m = JsonPath.read(json, "$.MSGID");
				String f = JsonPath.read(json, "$.PATH");
				hm.put("MSGID", m);
				hm.put("FILENAME", f);
			}
		});
		azureBlobUploadTask.setup(l,p);
		String msgId = hm.get("MSGID");
        fileName=hm.get("FILENAME");
        
        String filepath=fileName;

        if(l.isInfoEnabled())
            l.info("filapth in upload bolt{} and name is {}",filepath,fileName);


        HashMap<String, String> map = new HashMap();
        map.put(AbstractTask.DEFAULT_KEY, filepath);

        Float blobRes = azureBlobUploadTask.doTask(map);

// TODO: previous check      if(res==1)

        if(res!=null ) {
            if(blobRes!=Float.MIN_VALUE) {
                //collector.emit(new Values(msgId,fileName));
            	jobj.put("MSGID", msgId);
        		jobj.put("FILENAME", fileName);
        		
        		flowFile = session.write(flowFile, new OutputStreamCallback() {

        			public void process(OutputStream out) throws IOException {
        				out.write(jobj.toJSONString().getBytes());
        			}
        		});
            }
            else {
                if (l.isWarnEnabled()) l.warn("Error in AzureBlobUploadTaskBolt");
                throw new RuntimeException();
            }
        }
		session.transfer(flowFile, success);
		
	}
}