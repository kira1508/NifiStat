package org.apache.nifi.Hap;

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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;
//import com.sun.xml.internal.bind.v2.TODO;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.parse.SenMLParse;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.io.PrintWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Tags({ "SenML" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class SenML extends AbstractProcessor {

	public static final PropertyDescriptor PARSE_META_FIELD_SCHEMA = new PropertyDescriptor.Builder().name("MetaField")
			.displayName("MetaFieldSchema").description("Parse meta field schema").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PARSE_ID_FIELD_SCHEMA = new PropertyDescriptor.Builder().name("idField")
			.displayName("idFieldSchema").description("Parse id field schema").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PARSE_CSV_SCHEMA_FILEPATH = new PropertyDescriptor.Builder().name("csvField")
			.displayName("csvFieldSchemaFilepath").description("Parse csv field schema").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PARSE_SENML_USE_MSG_FIELD = new PropertyDescriptor.Builder()
			.name("senMlUseField").displayName("senMlUseFieldS").description("Parse sen to ml field on user msg")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PARSE_SENML_SAMPLEDATA = new PropertyDescriptor.Builder()
			.name("senMlSampleData").displayName("senMlSampleData").description("Sen to ML sample data").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	private ArrayList<String> observableFields;
	SenMLParse senMLParseTask;
	private Properties p;
	private String[] metaFields;
	private String idField;
	private static Logger l;
	private StringBuilder meta;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(PARSE_META_FIELD_SCHEMA);
		descriptors.add(PARSE_ID_FIELD_SCHEMA);
		descriptors.add(PARSE_CSV_SCHEMA_FILEPATH);
		descriptors.add(PARSE_SENML_SAMPLEDATA);
		descriptors.add(PARSE_SENML_USE_MSG_FIELD);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(success);
		this.relationships = Collections.unmodifiableSet(relationships);

	}

	public static void initLogger(Logger l_) {
		l = l_;
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
			p = new Properties();
			initLogger(LoggerFactory.getLogger("APP"));
			senMLParseTask = new SenMLParse();

			p.setProperty("PARSE.SENML.SAMPLEDATA", context.getProperty(PARSE_SENML_SAMPLEDATA).getValue());
			p.setProperty("PARSE.SENML.USE_MSG_FIELD", context.getProperty(PARSE_SENML_USE_MSG_FIELD).getValue());
			p.setProperty("PARSE.META_FIELD_SCHEMA", context.getProperty(PARSE_META_FIELD_SCHEMA).getValue());
			p.setProperty("PARSE.ID_FIELD_SCHEMA", context.getProperty(PARSE_ID_FIELD_SCHEMA).getValue());
			p.setProperty("PARSE.CSV_SCHEMA_FILEPATH", context.getProperty(PARSE_CSV_SCHEMA_FILEPATH).getValue());
			senMLParseTask.setup(l, p);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

		FlowFile flowFile = session.get();
		if (flowFile == null) {
			return;
		}
		final JSONObject flowFileInOutJson = new JSONObject();
		observableFields = new ArrayList<String>();
		session.read(flowFile, new InputStreamCallback() {
			public void process(InputStream in) throws IOException {
				String json = IOUtils.toString(in);
				List<Object> lobj = JsonPath.read(json, "$.PAYLOAD.e[*]");
				Long btlon = JsonPath.read(json, "$.PAYLOAD.bt");
				// String msg = lobj.toString();
				flowFileInOutJson.put("e", lobj);
				flowFileInOutJson.put("bt", String.valueOf(btlon));

			}
		});

		session.remove(flowFile);
		String line;
		ArrayList<String> metaList = new ArrayList<String>();
		idField = p.getProperty("PARSE.ID_FIELD_SCHEMA").toString();
		metaFields = p.getProperty("PARSE.META_FIELD_SCHEMA").toString().split(",");
		for (int i = 0; i < metaFields.length; i++) {
			metaList.add(metaFields[i]);
		}
		try {
			FileReader reader = new FileReader(p.getProperty("PARSE.CSV_SCHEMA_FILEPATH").toString());
			BufferedReader br = new BufferedReader(reader);
			line = br.readLine();
			String[] obsType = line.split(",");
			for (int i = 0; i < obsType.length; i++) {
				if (metaList.contains(obsType[i]) == false) {
					observableFields.add(obsType[i]);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		

		List<Object> msg = (List<Object>) flowFileInOutJson.get("e");
		String bt = (String) flowFileInOutJson.get("bt");
		flowFileInOutJson.clear();
		flowFileInOutJson.put("e", msg);
		flowFileInOutJson.put("bt", bt);
		final String msgid = flowFile.getAttribute("uuid");
		String sen = flowFileInOutJson.toJSONString().toString();
		sen = sen.replaceAll("\\\\", "");
		final HashMap<String, String> map = new HashMap<String, String>();
		map.put(AbstractTask.DEFAULT_KEY, sen);
		senMLParseTask.doTask(map);
		final HashMap<String, String> resultMap = (HashMap<String, String>) senMLParseTask.getLastResult();
		meta = new StringBuilder();
		
		flowFileInOutJson.clear();
		for (int i = 0; i < metaFields.length; i++) {
			meta.append(resultMap.get((metaFields[i]))).append(",");
		}
		meta = meta.deleteCharAt(meta.lastIndexOf(","));
		
		List<FlowFile> flowFiles = new ArrayList<FlowFile>();
		for (int i = 0; i < observableFields.size(); i++) {
			FlowFile tempFlow = session.create();
			flowFiles.add(tempFlow);
		}

		

		for (int j = 0; j < observableFields.size(); j++) {
			flowFileInOutJson.clear();
			flowFileInOutJson.put("MSGID", msgid);
			flowFileInOutJson.put("SENSORID", resultMap.get(idField));
			flowFileInOutJson.put("META", meta.toString());
			flowFileInOutJson.put("OBSTYPE", (String) observableFields.get(j));
			flowFileInOutJson.put("OBSVALUE", resultMap.get((String) observableFields.get(j)));

			FlowFile currFile = flowFiles.get(j);
			currFile = session.append(currFile, new OutputStreamCallback() {

				@Override
				public void process(OutputStream out) throws IOException {
					out.write(flowFileInOutJson.toJSONString().getBytes());
				}
			});
			flowFiles.set(j, currFile);

		}
		for (FlowFile x : flowFiles)
			session.transfer(x, success);
	}
}
