package org.apache.nifi.AverageProcessor;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.BlockWindowAverage;
import net.minidev.json.JSONObject;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.spi.LoggerFactory;
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

import com.jayway.jsonpath.JsonPath;
//import com.sun.xml.internal.bind.v2.TODO;

/**
 * Hello world!
 *
 */
@SuppressWarnings("unused")
public class AverageProcessor extends AbstractProcessor {

	public static final PropertyDescriptor AGGREGATE_BLOCK_AVERAGE_USE_MSG_FIELD = new PropertyDescriptor.Builder()
			.name("AverageUseMsgField").displayName("AverageUserMessageField").description("Message_Field").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor AGGREGATE_BLOCK_COUNT_WINDOW_SIZE = new PropertyDescriptor.Builder()
			.name("CountWindowSize").displayName("BlockCountWindowSize").description("Sizes").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final PropertyDescriptor AGGREGATE_BLOCK_COUNT_USE_MSG_FIELD = new PropertyDescriptor.Builder()
			.name("CountUseMsgField").displayName("CountUserMessageField").description("Message_Field").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();
	public static final Relationship failure = new Relationship.Builder().name("failure").description("On success")
			.build();
	
	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	private Properties p;
	private ArrayList<String> useMsgList;
	Map<String, BlockWindowAverage> blockWindowAverageMap;
	Logger l;
	// final ComponentLog logger = getLogger();

	@Override
	protected void init(ProcessorInitializationContext context) {
		// TODO Auto-generated method stub
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		final Set<Relationship> relationships = new HashSet<Relationship>();
		descriptors.add(AGGREGATE_BLOCK_AVERAGE_USE_MSG_FIELD);
		descriptors.add(AGGREGATE_BLOCK_COUNT_WINDOW_SIZE);
		descriptors.add(AGGREGATE_BLOCK_COUNT_USE_MSG_FIELD);
		this.descriptors = Collections.unmodifiableList(descriptors);

		relationships.add(success);
		relationships.add(failure);
		this.relationships = Collections.unmodifiableSet(relationships);

		super.init(context);
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
	public void OnScheduled(final ProcessContext context) {
		try {
			p = new Properties();
			blockWindowAverageMap = new HashMap<String, BlockWindowAverage>();
			useMsgList = new ArrayList<String>();
			p.setProperty("AGGREGATE.BLOCK_AVERAGE.USE_MSG_FIELD",
					context.getProperty(AGGREGATE_BLOCK_AVERAGE_USE_MSG_FIELD).getValue());
			p.setProperty("AGGREGATE.BLOCK_COUNT.WINDOW_SIZE",
					context.getProperty(AGGREGATE_BLOCK_COUNT_WINDOW_SIZE).getValue());
			p.setProperty("AGGREGATE.BLOCK_COUNT.USE_MSG_FIELD",
					context.getProperty(AGGREGATE_BLOCK_COUNT_USE_MSG_FIELD).getValue());
					} catch (Exception e) {
			System.out.println(e.getStackTrace());
		}

	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		FlowFile flowFile = session.get();
		FlowFile flowfilesucc;
		if (flowFile == null) {
			return;
		}
		final JSONObject Jobj = new JSONObject();

		// p.setProperty("AGGREGATE.BLOCK_AVERAGE.USE_MSG_FIELD",
		// context.getProperty(AGGREGATE_BLOCK_AVERAGE_USE_MSG_FIELD).getValue());
		session.read(flowFile, new InputStreamCallback() {

			@Override
			public void process(InputStream in) throws IOException {
				// TODO Auto-generated method stub
				String json = IOUtils.toString(in);
				String msgID = JsonPath.read(json, "$.MSGID");
				String sensorMeta = JsonPath.read(json, "$.META");
				String sensorID = JsonPath.read(json, "$.SENSORID");
				String obsType = JsonPath.read(json, "$.OBSTYPE");
				String obsVal = JsonPath.read(json, "$.OBSVALUE");
				Jobj.put("MSGID", msgID);
				Jobj.put("META", sensorMeta);
				Jobj.put("SENSORID", sensorID);
				Jobj.put("OBSTYPE", obsType);
				Jobj.put("OBSVALUE", obsVal);
			}
		});
		session.remove(flowFile);
		flowFile = session.create();
		/*
		 * flowFile = session.write(flowFile, new OutputStreamCallback() {
		 * 
		 * @Override public void process(OutputStream out) throws IOException { // TODO
		 * Auto-generated method stub out.write(prolist.toJSONString().getBytes()); for
		 * (String e: useMsgList) { out.write(e.getBytes()); }
		 * //out.write(p.toString().getBytes());
		 * 
		 * //session.transfer(flowFile,success); } });
		 */
		String msgID = (String) Jobj.get("MSGID");
		String obsType = (String) Jobj.get("OBSTYPE");
		String sensorID = (String) Jobj.get("SENSORID");
		String sensorMeta = (String) Jobj.get("META");
		String obsVal = (String) Jobj.get("OBSVALUE");
		Jobj.clear();
		
		l = org.slf4j.LoggerFactory.getLogger("APP");
		String useMsgField = p.getProperty("AGGREGATE.BLOCK_AVERAGE.USE_MSG_FIELD");
		String[] msgField = useMsgField.split(",");
		//useMsgList = new ArrayList<String>();

		for (String s : msgField) {
			useMsgList.add(s);
		}

		
		if (useMsgList.contains(obsType)) {

			String key = sensorID + obsType;
			BlockWindowAverage blockWindowAverage = blockWindowAverageMap.get(key);

			if (blockWindowAverage == null) {
				blockWindowAverage = new BlockWindowAverage();
				blockWindowAverage.setup(l, p);
				blockWindowAverageMap.put(key, blockWindowAverage);
			}

			final HashMap<String, String> map = new HashMap<String, String>();

			map.put(AbstractTask.DEFAULT_KEY, obsVal);
			final Float res = blockWindowAverage.doTask(map);
			
			/*flowFile = session.write(flowFile, new OutputStreamCallback() {
				
				@Override
				public void process(OutputStream out) throws IOException {
					// TODO Auto-generated method stub
					
					for (Object e: map.keySet())
					{
						out.write(map.get(e.toString()).toString().getBytes());
				}
					
				}
			});*/

			sensorMeta = sensorMeta.concat(",").concat(obsType);
			obsType = "AVG";
			if (res != null) {
				if (res != Float.MIN_VALUE)

				{
					flowfilesucc=session.create();
					Jobj.clear();
					Jobj.put("MSGID", msgID);
					Jobj.put("META", sensorMeta);
					Jobj.put("SENSORID", sensorID);
					Jobj.put("OBSTYPE", obsType);
					Jobj.put("res", res.toString());
					flowfilesucc = session.write(flowfilesucc, new OutputStreamCallback() {

						@Override
						public void process(OutputStream out) throws IOException {
							// TODO Auto-generated method stub
							out.write(Jobj.toJSONString().getBytes());
							// out.write("HeyThere".getBytes());

							// session.transfer(flowFile,success);
						}
					});
					session.transfer(flowfilesucc, success);

				} 
			}

				
		}
		
		session.transfer(flowFile, failure);
	}

}
	
