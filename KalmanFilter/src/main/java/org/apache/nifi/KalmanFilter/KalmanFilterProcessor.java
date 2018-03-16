package org.apache.nifi.KalmanFilter;

/**
 * Hello world!
 *
 */
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

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.statistics.KalmanFilter;
import net.minidev.json.JSONObject;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class KalmanFilterProcessor extends AbstractProcessor {

	public static final PropertyDescriptor STATISTICS_KALMAN_FILTER_USE_MSG_FIELDLIST = new PropertyDescriptor.Builder()
			.name("UseMsgFieldList").displayName("UseMsgFieldList")
			.description("STATISTICS KALMAN FILTER USE MSG FIELDLIST").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
	
	public static final PropertyDescriptor STATISTICS_KALMAN_FILTER_USE_MSG_FIELD = new PropertyDescriptor.Builder()
			.name("UseMsgField").displayName("UseMsgFieldList")
			.description("STATISTICS KALMAN FILTER USE MSG FIELD").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor STATISTICS_KALMAN_FILTER_PROCESS_NOISE = new PropertyDescriptor.Builder()
			.name("ProcessNoise").displayName("Kalman Filter Process noise")
			.description("STATISTICS KALMAN FILTER PROCESS NOISE").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor STATISTICS_KALMAN_FILTER_SENSOR_NOISE = new PropertyDescriptor.Builder()
			.name("SensorNoise").displayName("Kalman Filter Sensor noise")
			.description("STATISTICS KALMAN FILTER SENSOR NOISE").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor STATISTICS_KALMAN_FILTER_ESTIMATED_ERROR = new PropertyDescriptor.Builder()
			.name("EstimatedError").displayName("Kalman Filter estimated error")
			.description("STATISTICS KALMAN FILTER SESTIMATED ERROR").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();

	Map<String, KalmanFilter> kmap;
	private Properties p;
	Logger l;
	private ArrayList<String> useMsgList;

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final ComponentLog log = getLogger();
		log.debug("Init");
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(STATISTICS_KALMAN_FILTER_USE_MSG_FIELDLIST);
		descriptors.add(STATISTICS_KALMAN_FILTER_PROCESS_NOISE);
		descriptors.add(STATISTICS_KALMAN_FILTER_SENSOR_NOISE);
		descriptors.add(STATISTICS_KALMAN_FILTER_ESTIMATED_ERROR);
		descriptors.add(STATISTICS_KALMAN_FILTER_USE_MSG_FIELD);
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
		final ComponentLog logger = getLogger();
		try {
			logger.debug("Try block");
			p = new Properties();
			p.setProperty("STATISTICS.KALMAN_FILTER.USE_MSG_FIELDLIST",
					context.getProperty(STATISTICS_KALMAN_FILTER_USE_MSG_FIELDLIST).getValue());
			p.setProperty("STATISTICS.KALMAN_FILTER.USE_MSG_FIELD",
					context.getProperty(STATISTICS_KALMAN_FILTER_USE_MSG_FIELD).getValue());
			p.setProperty("STATISTICS.KALMAN_FILTER.PROCESS_NOISE",
					context.getProperty(STATISTICS_KALMAN_FILTER_PROCESS_NOISE).getValue());
			p.setProperty("STATISTICS.KALMAN_FILTER.SENSOR_NOISE",
					context.getProperty(STATISTICS_KALMAN_FILTER_SENSOR_NOISE).getValue());
			p.setProperty("SSTATISTICS.KALMAN_FILTER.ESTIMATED_ERROR",
					context.getProperty(STATISTICS_KALMAN_FILTER_ESTIMATED_ERROR).getValue());
			kmap = new HashMap<String, KalmanFilter>();
			String useMsgField = p.getProperty("STATISTICS.KALMAN_FILTER.USE_MSG_FIELDLIST");
			String[] msgField = useMsgField.split(",");
			useMsgList = new ArrayList<String>();
			for (String s : msgField) {
				useMsgList.add(s);
			}
		} catch (Exception e) {
			logger.debug("Catch block");
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unused")
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		final HashMap<String, String> hm = new HashMap<String, String>();
		final JSONObject jobj = new JSONObject();
		if (flowFile == null) {
			return;
		}

		session.read(flowFile, new InputStreamCallback() {

			public void process(InputStream arg0) throws IOException {
				String json = IOUtils.toString(arg0, "UTF-8");
				String m = JsonPath.read(json, "$.MSGID");
				String sm = JsonPath.read(json, "$.META");
				String si = JsonPath.read(json, "$.SENSORID");
				String ot = JsonPath.read(json, "$.OBSTYPE");
				String ov = JsonPath.read(json, "$.OBSVALUE");
				hm.put("MSGID", m);
				hm.put("META", sm);
				hm.put("SENSORID", si);
				hm.put("OBSTYPE", ot);
				hm.put("OBSVALUE", ov);
			}
		});

		String msgId = (String) hm.get("MSGID");
		String sensorMeta = (String) hm.get("META");
		String sensorID = (String) hm.get("SENSORID");
		String obsType = (String) hm.get("OBSTYPE");
		String obsVal = (String) hm.get("OBSVALUE");
		String val = null;

		l = LoggerFactory.getLogger("APP");

		if (useMsgList.contains(obsType)) {
			String key = sensorID + obsType;

			KalmanFilter kalmanFilter = kmap.get(key);
			if (kalmanFilter == null) {
				kalmanFilter = new KalmanFilter();
				kalmanFilter.setup(l, p);
				kmap.put(key, kalmanFilter);
			}

			HashMap<String, String> map = new HashMap<String, String>();
			map.put(AbstractTask.DEFAULT_KEY, obsVal);
			@SuppressWarnings("unchecked")
			Float kalmanUpdatedVal = kalmanFilter.doTask(map);
			val = kalmanUpdatedVal.toString();

			if (kalmanUpdatedVal != null) {
				// collector.emit(new Values(sensorMeta, sensorID, obsType,
				// kalmanUpdatedVal.toString(), msgId));
				jobj.put("MSGID", msgId);
				jobj.put("META", sensorMeta);
				jobj.put("SENSORID", sensorID);
				jobj.put("OBSTYPE", obsType);
				jobj.put("kalmanUpdatedVal", Float.toString(kalmanUpdatedVal));

				flowFile = session.write(flowFile, new OutputStreamCallback() {

					public void process(OutputStream out) throws IOException {
						out.write(jobj.toJSONString().getBytes());
					}
				});

			} else {
				// if (l.isWarnEnabled()) l.warn("Error in KalmanFilterBolt and
				// Val is -"+kalmanUpdatedVal);
				throw new RuntimeException();
			}
		}

		session.transfer(flowFile, success);
	}

	/*
	 * @Override public void declareOutputFields(OutputFieldsDeclarer
	 * outputFieldsDeclarer) { outputFieldsDeclarer.declare(new
	 * Fields("META","SENSORID","OBSTYPE","kalmanUpdatedVal","MSGID")); }
	 */
}