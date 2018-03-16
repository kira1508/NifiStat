package org.apache.nifi.MultilinePlot;

import in.dream_lab.bm.stream_iot.tasks.AbstractTask;
import in.dream_lab.bm.stream_iot.tasks.aggregate.AccumlatorTask;
import in.dream_lab.bm.stream_iot.tasks.io.ZipMultipleBufferTask;
import in.dream_lab.bm.stream_iot.tasks.utils.TimestampValue;
import in.dream_lab.bm.stream_iot.tasks.visualize.XChartMultiLinePlotTask;
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
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;
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
import com.jayway.jsonpath.JsonPath;

/**
 * Hello world!
 *
 */
@SuppressWarnings("unused")
public class MultilinePlot extends AbstractProcessor {
	private Properties p;
	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	XChartMultiLinePlotTask plotTask;
	AccumlatorTask accumlatorTask;
	ZipMultipleBufferTask zipTask;
	org.slf4j.Logger l;
	public static final PropertyDescriptor AGGREGATE_ACCUMLATOR_MULTIVALUE_OBSTYPE = new PropertyDescriptor.Builder()
			.name("MultiValueObstype").displayName("MultiValue_OBSTYPE").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor AGGREGATE_ACCUMLATOR_TUPLE_WINDOW_SIZE = new PropertyDescriptor.Builder()
			.name("Window_Size").displayName("Window_Size").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor AGGREGATE_ACCUMLATOR_META_TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
			.name("Aggregate_Accumulator").displayName("Time_Stamp").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor IO_ZIPBUFFER_MAX_FILES_COUNT = new PropertyDescriptor.Builder()
			.name("Io_ZipBuffer_Max_Files").displayName("Max_file_count").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor IO_ZIPBUFFER_OUTPUT_ZIP_FILE_PATH = new PropertyDescriptor.Builder()
			.name("Io_ZipBuffer_File_Path").displayName("Zip_File_path").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor IO_ZIPBUFFER_FILENAME_PATTERN = new PropertyDescriptor.Builder()
			.name("Io_ZipBuffer_File_Name").displayName("Zip_Filename_pattern").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor IO_ZIPBUFFER_FILENAME_EXT = new PropertyDescriptor.Builder()
			.name("Io_ZipBuffer_File_Ext").displayName("Zip_Filename_Ext").description("").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship success = new Relationship.Builder().name("success").description("On success")
			.build();

	public static final Relationship failure = new Relationship.Builder().name("failure").description("On Failure")
			.build();

	@Override
	protected void init(ProcessorInitializationContext context) {
		// TODO Auto-generated method stub

		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();

		descriptors.add(AGGREGATE_ACCUMLATOR_META_TIMESTAMP_FIELD);
		descriptors.add(AGGREGATE_ACCUMLATOR_TUPLE_WINDOW_SIZE);
		descriptors.add(AGGREGATE_ACCUMLATOR_MULTIVALUE_OBSTYPE);
		descriptors.add(IO_ZIPBUFFER_FILENAME_EXT);
		descriptors.add(IO_ZIPBUFFER_FILENAME_PATTERN);
		descriptors.add(IO_ZIPBUFFER_MAX_FILES_COUNT);
		descriptors.add(IO_ZIPBUFFER_OUTPUT_ZIP_FILE_PATH);
		this.descriptors = Collections.unmodifiableList(descriptors);
		final Set<Relationship> relationships = new HashSet<Relationship>();
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
			p.setProperty("AGGREGATE.ACCUMLATOR.MULTIVALUE_OBSTYPE",
					context.getProperty(AGGREGATE_ACCUMLATOR_MULTIVALUE_OBSTYPE).getValue());
			p.setProperty("AGGREGATE.ACCUMLATOR.TUPLE_WINDOW_SIZE",
					context.getProperty(AGGREGATE_ACCUMLATOR_TUPLE_WINDOW_SIZE).getValue());
			p.setProperty("AGGREGATE.ACCUMLATOR.META_TIMESTAMP_FIELD",
					context.getProperty(AGGREGATE_ACCUMLATOR_META_TIMESTAMP_FIELD).getValue());
			p.setProperty("IO.ZIPBUFFER.MAX_FILES_COUNT", context.getProperty(IO_ZIPBUFFER_MAX_FILES_COUNT).getValue());
			p.setProperty("IO.ZIPBUFFER.OUTPUT_ZIP_FILE_PATH",
					context.getProperty(IO_ZIPBUFFER_OUTPUT_ZIP_FILE_PATH).getValue());
			p.setProperty("IO.ZIPBUFFER.FILENAME_PATTERN",
					context.getProperty(IO_ZIPBUFFER_FILENAME_PATTERN).getValue());
			p.setProperty("IO.ZIPBUFFER.FILENAME_EXT", context.getProperty(IO_ZIPBUFFER_FILENAME_EXT).getValue());

			plotTask = new XChartMultiLinePlotTask();
			accumlatorTask = new AccumlatorTask();
			zipTask = new ZipMultipleBufferTask();
			l = org.slf4j.LoggerFactory.getLogger("App");
			accumlatorTask.setup(l, p);
			plotTask.setup(l, p);
			zipTask.setup(l, p);

		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		// TODO Auto-generated method stub

		FlowFile flowFile = session.get();
		FlowFile Flowfilesucc;
		if (flowFile == null) {
			return;
		}

		final JSONObject jobj = new JSONObject();

		session.read(flowFile, new InputStreamCallback() {

			public void process(InputStream in) throws IOException {
				// TODO Auto-generated method stub

				String json = IOUtils.toString(in);
				String MSGID = JsonPath.read(json, "$.MSGID");
				String META = JsonPath.read(json, "$.META");
				String SENSORID = JsonPath.read(json, "$.SENSORID");
				String OBSTYPE = JsonPath.read(json, "$.OBSTYPE");
				String OBSVALUE = JsonPath.read(json, "$.res");
				// Map<String, String> map = new HashMap<String, String>();
				jobj.put("MSGID", MSGID);
				jobj.put("SENSORID", SENSORID);
				jobj.put("OBSTYPE", OBSTYPE);
				jobj.put("OBSVALUE", OBSVALUE);
				jobj.put("META", META);

			}

		});

		String MSGID = (String) jobj.get("MSGID");
		String META = (String) jobj.get("META");
		String SENSORID = (String) jobj.get("SENSORID");
		String OBSTYPE = (String) jobj.get("OBSTYPE");
		String OBSVALUE = (String) jobj.get("OBSVALUE");

		session.remove(flowFile);
		flowFile = session.create();

		final Map<String, String> map = new HashMap<String, String>();
		map.put("SENSORID", SENSORID);
		map.put("OBSTYPE", OBSTYPE);
		map.put("OBSVALUE", OBSVALUE);
		map.put("META", META);

		float res = accumlatorTask.doTaskLogic(map);

		if (res == 1.0f) {
			try {
				// get accumulated values
				final Map<String, Map<String, Queue<TimestampValue>>> valuesMap = accumlatorTask.getLastResult();

				Set<Entry<String, Map<String, Queue<TimestampValue>>>> entrySet = valuesMap.entrySet();

				// For each type of accumulated observation
				for (Entry<String, Map<String, Queue<TimestampValue>>> entry : entrySet) {
					// send accumulated values for observation to plotting routine, in-memory
					// and get an input stream with response
					Map<String, Queue<TimestampValue>> inputForPlotMap = entry.getValue();
					plotTask.doTaskLogic(inputForPlotMap);
					InputStream byteInputStream = plotTask.getLastResult();

					// send generated chart from input stream, and send to zip task
					HashMap<String, InputStream> inputForZipMap = new HashMap<String, InputStream>();
					inputForZipMap.put(AbstractTask.DEFAULT_KEY, byteInputStream);
					final Float zipres = zipTask.doTask(inputForZipMap);
					// if zip is done batching one set of requests, send zip path downstream

					if (zipres == 1.0f) {
						// emit the path sent as last result from zip task
						Flowfilesucc = session.create();
						String path = zipTask.getLastResult();
						path = path.replaceAll("\\\\", "");
						jobj.clear();
						jobj.put("MSGID", MSGID);
						jobj.put("PATH", path);
						Flowfilesucc = session.write(Flowfilesucc, new OutputStreamCallback() {

							public void process(OutputStream out) throws IOException {
								// TODO Auto-generated method stub

								out.write(jobj.toJSONString().getBytes());

							}
						});

						session.transfer(Flowfilesucc, success);

					}
				}
			} catch (Exception e) {
				l.error("Exception occured in MultiLinePlotBolt exceute method" + e.getMessage());
			}

		}

		session.transfer(flowFile, failure);
	}
}
