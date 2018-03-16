package org.apache.nifi.SimpleLinearRegression;

/**
 * Hello world!
 *
 */
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.commons.io.IOUtils;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.JsonPath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.io.PrintWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.predict.SimpleLinearRegressionPredictor;
import net.minidev.json.JSONObject;

@Tags({ "example" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })

public class SimpleLinearRegressionProcessor extends AbstractProcessor {
	public static final PropertyDescriptor PREDICT_SIMPLE_LINEAR_REGRESSION_USE_MSG_FIELD = new PropertyDescriptor.Builder()
			.name("UseMsgField").displayName("UseMsgField")
			.description("Predict simple linear regression use msg field").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PREDICT_SIMPLE_LINEAR_REGRESSION_WINDOW_SIZE_TRAIN = new PropertyDescriptor.Builder()
			.name("WindowSizeTrain").displayName("WindowSizeTrain").description("Window Size Train").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PREDICT_SIMPLE_LINEAR_REGRESSION_WINDOW_SIZE_PREDICT = new PropertyDescriptor.Builder()
			.name("WindowSizePredict").displayName("WindowSizePredict").description("Predict window size")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship success = new Relationship.Builder().name("success").description("success")
			.build();

	private Properties p;
	Map<String, SimpleLinearRegressionPredictor> slrmap;
	Logger l;

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(PREDICT_SIMPLE_LINEAR_REGRESSION_USE_MSG_FIELD);
		descriptors.add(PREDICT_SIMPLE_LINEAR_REGRESSION_WINDOW_SIZE_TRAIN);
		descriptors.add(PREDICT_SIMPLE_LINEAR_REGRESSION_WINDOW_SIZE_PREDICT);

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
			slrmap = new HashMap<String, SimpleLinearRegressionPredictor>();
			p.setProperty("PREDICT.SIMPLE_LINEAR_REGRESSION.USE_MSG_FIELD",
					context.getProperty(PREDICT_SIMPLE_LINEAR_REGRESSION_USE_MSG_FIELD).getValue());
			p.setProperty("PREDICT.SIMPLE_LINEAR_REGRESSION.WINDOW_SIZE_TRAIN",
					context.getProperty(PREDICT_SIMPLE_LINEAR_REGRESSION_WINDOW_SIZE_TRAIN).getValue());
			p.setProperty("PREDICT.SIMPLE_LINEAR_REGRESSION.WINDOW_SIZE_PREDICT",
					context.getProperty(PREDICT_SIMPLE_LINEAR_REGRESSION_WINDOW_SIZE_PREDICT).getValue());
		} catch (Exception e) {
			logger.debug("Catch block");
			e.printStackTrace();
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();

		final HashMap<String, String> hm = new HashMap<String, String>();
		final JSONObject jobj = new JSONObject();
		if (flowFile == null) {
			return;
		}
		// TODO implement

		session.read(flowFile, new InputStreamCallback() {

			public void process(InputStream arg0) throws IOException {
				String json = IOUtils.toString(arg0, "UTF-8");
				String m = JsonPath.read(json, "$.MSGID");
				String sm = JsonPath.read(json, "$.META");
				String si = JsonPath.read(json, "$.SENSORID");
				String ot = JsonPath.read(json, "$.OBSTYPE");
				String kuv = JsonPath.read(json, "$.kalmanUpdatedVal");
				hm.put("MSGID", m);
				hm.put("META", sm);
				hm.put("SENSORID", si);
				hm.put("OBSTYPE", ot);
				hm.put("kalmanUpdatedVal", kuv);
			}
		});
		session.remove(flowFile);

		String msgId = (String) hm.get("MSGID");
		String sensorMeta = (String) hm.get("META");
		String sensorID = (String) hm.get("SENSORID");
		String obsType = (String) hm.get("OBSTYPE");
		String kalmanUpdatedVal = (String) hm.get("kalmanUpdatedVal");
		String key = sensorID + obsType;

		l = LoggerFactory.getLogger("APP");

		SimpleLinearRegressionPredictor simpleLinearRegressionPredictor = slrmap.get(key);
		if (simpleLinearRegressionPredictor == null) {
			simpleLinearRegressionPredictor = new SimpleLinearRegressionPredictor();
			simpleLinearRegressionPredictor.setup(l, p);
			slrmap.put(key, simpleLinearRegressionPredictor);
		}
		final HashMap<String, String> map = new HashMap<String, String>();
		map.put(AbstractTask.DEFAULT_KEY, kalmanUpdatedVal);
		simpleLinearRegressionPredictor.doTask(map);
		final float[] res = simpleLinearRegressionPredictor.getLastResult();
		/*
		 * flowFile = session.write(flowFile, new OutputStreamCallback() {
		 * 
		 * @Override public void process(OutputStream out) throws IOException { // TODO
		 * Auto-generated method stub
		 * 
		 * for(Object e:map.keySet()) {
		 * out.write(map.get(e.toString()).toString().getBytes()); }
		 * 
		 * out.write(res.toString().getBytes());
		 * 
		 * } });
		 */

		if (res != null) {
			flowFile = session.create();
			StringBuffer resTostring = new StringBuffer();

			for (int c = 0; c < res.length; c++) {
				resTostring.append(res[c]);
				resTostring.append("#");
			}

			sensorMeta = sensorMeta.concat(",").concat(obsType);
			obsType = "SLR";
			// collector.emit(new Values(sensorID ,sensorMeta, obsType,
			// resTostring.toString(), msgId));

			jobj.put("MSGID", msgId);
			jobj.put("META", sensorMeta);
			jobj.put("SENSORID", sensorID);
			jobj.put("OBSTYPE", obsType);
			jobj.put("res", resTostring.toString());

			flowFile = session.write(flowFile, new OutputStreamCallback() {

				public void process(OutputStream out) throws IOException {
					out.write(jobj.toJSONString().getBytes());
				}
			});

			session.transfer(flowFile, success);
		}

	}

}
