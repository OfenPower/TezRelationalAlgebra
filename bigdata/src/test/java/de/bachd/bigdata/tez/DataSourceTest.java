package de.bachd.bigdata.tez;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

/**
 * Hello World!
 *
 */
public class DataSourceTest extends Configured implements Tool {

	public static class DataProcessor extends SimpleProcessor {

		public DataProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			for (int i = 0; i < 5; i++) {
				System.out.println("DataProcessor run()");
			}
		}
	}

	public static class DataOutput extends AbstractLogicalOutput {

		public DataOutput(OutputContext outputContext, int numPhysicalOutputs) {
			super(outputContext, numPhysicalOutputs);
			System.out.println("------Output Constructor------");
		}

		@Override
		public void start() throws Exception {
			System.out.println("------Output start-------");
		}

		@Override
		public Writer getWriter() throws Exception {
			System.out.println("------Output getReader-------");
			return null;
		}

		@Override
		public void handleEvents(List<Event> outputEvents) {
			System.out.println("------Output handleEvents-------");

		}

		@Override
		public List<Event> close() throws Exception {
			System.out.println("------Output close-------");
			return null;
		}

		@Override
		public List<Event> initialize() throws Exception {
			System.out.println("------Output initialize-------");
			return null;
		}
	}

	public static class DataInput extends AbstractLogicalInput {

		public DataInput(InputContext inputContext, int numPhysicalInputs) {
			super(inputContext, numPhysicalInputs);
			System.out.println("------Input Constructor------");
		}

		@Override
		public void start() throws Exception {
			System.out.println("------Input start-------");
		}

		@Override
		public Reader getReader() throws Exception {
			System.out.println("------Input getReader-------");
			return null;
		}

		@Override
		public void handleEvents(List<Event> inputEvents) throws Exception {
			System.out.println("------Input handleEvents-------");
		}

		@Override
		public List<Event> close() throws Exception {
			System.out.println("------Input close-------");
			return null;
		}

		@Override
		public List<Event> initialize() throws Exception {
			System.out.println("------Input initialize-------");
			return null;
		}
	}

	public static class SchemeProcessor extends SimpleProcessor {

		public SchemeProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			for (int i = 0; i < 5; i++) {
				System.out.println("SchemeProcessor run()");
			}
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		// Tez lokal aufsetzen
		TezConfiguration tezConf = new TezConfiguration(getConf());
		tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
		tezConf.set("fs.default.name", "file:///");
		tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

		// TezClient starten
		TezClient tezClient = TezClient.create("TezClient", tezConf);
		tezClient.start();

		return runJob(args, tezConf, tezClient);
	}

	private int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient) throws TezException, IOException {

		// Credentials cred = Credentials.readTokenStorageFile(arg0, arg1);
		DataSourceDescriptor ds = DataSourceDescriptor.create(InputDescriptor.create(DataInput.class.getName()), null,
				null);

		// Knoten erzeugen
		Vertex v1 = Vertex.create("v1", ProcessorDescriptor.create(DataProcessor.class.getName()));
		v1.addDataSource("input", ds);

		// conf.set(FileInputFormat.INPUT_DIR, inputPaths);

		// Graph erzeugen
		DAG dag = DAG.create("dag");
		dag.addVertex(v1);
		tezClient.submitDAG(dag);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new DataSourceTest(), args);
	}
}
