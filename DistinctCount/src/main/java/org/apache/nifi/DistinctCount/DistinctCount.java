package org.apache.nifi.DistinctCount;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.DistinctApproxCount;
import net.minidev.json.JSONObject;

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

//import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.jayway.jsonpath.JsonPath;

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

/**
 * Hello world!
 *
 */
@SuppressWarnings("unused")
public class DistinctCount extends AbstractProcessor {
	private Properties p;
	DistinctApproxCount distinctApproxCount;
	Logger l;

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	public String useMsgField;

	public static final PropertyDescriptor AGGREGATE_DISTINCT_APPROX_COUNT_USE_MSG_FIELD = new PropertyDescriptor.Builder()
			.name("DistinctMsgCount").displayName("Distinct_MSG_Count").description("Provide Distinct Msg Count")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor AGGREGATE_DISTINCT_APPROX_COUNT_BUCKETS = new PropertyDescriptor.Builder()
			.name("Count").displayName("bucket").description("bucket_count").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();

	@Override
	protected void init(ProcessorInitializationContext context) {
		// TODO Auto-generated method stub

		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(AGGREGATE_DISTINCT_APPROX_COUNT_USE_MSG_FIELD);
		this.descriptors = Collections.unmodifiableList(descriptors);
		descriptors.add(AGGREGATE_DISTINCT_APPROX_COUNT_BUCKETS);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(success);

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
			distinctApproxCount = new DistinctApproxCount();
			p.setProperty("AGGREGATE.DISTINCT_APPROX_COUNT.USE_MSG_FIELD",
					context.getProperty(AGGREGATE_DISTINCT_APPROX_COUNT_USE_MSG_FIELD).getValue());
			p.setProperty("AGGREGATE.DISTINCT_APPROX_COUNT.BUCKETS",
					context.getProperty(AGGREGATE_DISTINCT_APPROX_COUNT_BUCKETS).getValue());
			l = LoggerFactory.getLogger("APP");

			distinctApproxCount.setup(l, p);
			useMsgField = p.getProperty("AGGREGATE.DISTINCT_APPROX_COUNT.USE_MSG_FIELD");

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e.getStackTrace());
		}
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub
		FlowFile flowFile = session.get();
		final JSONObject jobj = new JSONObject();
		// String msgID;
		ArrayList<String> msgtype = new ArrayList<String>();
		msgtype.add("fare_amount");
		msgtype.add("trip_distance");
		msgtype.add("trip_time_in_secs");
		if (flowFile == null) {
			return;
		}

		session.read(flowFile, new InputStreamCallback() {

			public void process(InputStream in) throws IOException {
				// TODO Auto-generated method stub
				String json = IOUtils.toString(in);
				String msgID = JsonPath.read(json, "$.MSGID");
				String sensorMeta = JsonPath.read(json, "$.META");
				String sensorID = JsonPath.read(json, "$.SENSORID");
				String obsType = JsonPath.read(json, "$.OBSTYPE");
				jobj.put("msgID", msgID);
				jobj.put("sensorMeta", sensorMeta);
				jobj.put("sensorID", sensorID);
				jobj.put("obsType", obsType);

			}
		});
		
		String msgID = (String) jobj.get("msgID");
		String sensorMeta = (String) jobj.get("sensorMeta");
		String sensorID = (String) jobj.get("sensorID");
		String obsType = (String) jobj.get("obsType");

		HashMap<String, String> map = new HashMap<String, String>();
		map.put(AbstractTask.DEFAULT_KEY, sensorID);
		Float res = null;

		if (msgtype.contains(obsType)) {
			distinctApproxCount.doTask(map);
			res = (Float) distinctApproxCount.getLastResult();
		}

		if (res != null) {
			sensorMeta = sensorMeta.concat(",").concat(obsType);
			obsType = "DA";
			if (res != Float.MIN_VALUE) {
				jobj.clear();
				jobj.put("MSGID", msgID);
				jobj.put("META", sensorMeta);
				jobj.put("SENSORID", sensorID);
				jobj.put("OBSTYPE", obsType);
				jobj.put("res",res.toString());

				flowFile = session.write(flowFile, new OutputStreamCallback() {

					@Override
					public void process(OutputStream out) throws IOException {
						// TODO Auto-generated method stub

						out.write(jobj.toJSONString().getBytes());

					}
				});
				// collector.emit(new Values(sensorMeta,sensorID,obsType,res.toString(),msgId));
			} else {
				/*
				 * if (l.isWarnEnabled()) l.warn("Error in distinct approx"); throw new
				 * RuntimeException();
				 */
			}
		}

		session.transfer(flowFile, success);

	}
}
