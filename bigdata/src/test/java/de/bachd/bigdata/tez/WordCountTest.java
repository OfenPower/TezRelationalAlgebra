package de.bachd.bigdata.tez;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.processor.SimpleMRProcessor;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.api.KeyValuesReader;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;
import org.apache.tez.runtime.library.processor.SimpleProcessor;

import com.google.common.base.Preconditions;

import de.bachd.bigdata.tez.wordcount.TezExampleBase;

/**
 * Hello World!
 *
 */
public class WordCountTest extends TezExampleBase {

	static String INPUT = "Input";
	static String OUTPUT = "Output";
	static String V1 = "v1";
	static String V2 = "v2";

	public static class TokenProcessor extends SimpleProcessor {
		IntWritable one = new IntWritable(1);
		Text word = new Text();

		public TokenProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getInputs().size() == 1);
			Preconditions.checkArgument(getOutputs().size() == 1);
			// the recommended approach is to cast the reader/writer to a
			// specific type instead
			// of casting the input/output. This allows the actual input/output
			// type to be replaced
			// without affecting the semantic guarantees of the data type that
			// are represented by
			// the reader and writer.
			// The inputs/outputs are referenced via the names assigned in the
			// DAG.
			System.out.println("TokenProcesser run()");
			KeyValueReader kvReader = (KeyValueReader) getInputs().get(INPUT).getReader();
			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(V2).getWriter();
			while (kvReader.next()) {
				StringTokenizer itr = new StringTokenizer(kvReader.getCurrentValue().toString());
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					// Count 1 every time a word is observed. Word is the key a
					// 1 is the value
					kvWriter.write(word, one);
				}
			}
		}
	}

	public static class SumProcessor extends SimpleMRProcessor {
		public SumProcessor(ProcessorContext context) {
			super(context);
		}

		@Override
		public void run() throws Exception {
			Preconditions.checkArgument(getInputs().size() == 1);
			Preconditions.checkArgument(getOutputs().size() == 1);
			System.out.println("SumProcesser run()");
			KeyValueWriter kvWriter = (KeyValueWriter) getOutputs().get(OUTPUT).getWriter();
			// The KeyValues reader provides all values for a given key. The
			// aggregation of values per key
			// is done by the LogicalInput. Since the key is the word and the
			// values are its counts in
			// the different TokenProcessors, summing all values per key
			// provides the sum for that word.
			KeyValuesReader kvReader = (KeyValuesReader) getInputs().get(V1).getReader();
			while (kvReader.next()) {
				Text word = (Text) kvReader.getCurrentKey();
				int sum = 0;
				for (Object value : kvReader.getCurrentValues()) {
					sum += ((IntWritable) value).get();
				}
				kvWriter.write(word, new IntWritable(sum));
			}
			// deriving from SimpleMRProcessor takes care of committing the
			// output
			// It automatically invokes the commit logic for the OutputFormat if
			// necessary.
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCountTest(), args);
	}

	@Override
	protected void printUsage() {
		System.err.println("Usage: " + " wordcount in out [numPartitions]");
	}

	@Override
	protected int validateArgs(String[] otherArgs) {
		if (otherArgs.length < 2 || otherArgs.length > 3) {
			return 2;
		}
		return 0;
	}

	@Override
	protected int runJob(String[] args, TezConfiguration tezConf, TezClient tezClient) throws Exception {
		// Datasource
		DataSourceDescriptor dataSource1 = MRInput
				.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, "artikel.csv").build();
		DataSinkDescriptor dataSink1 = MROutput
				.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, "output.csv").build();

		DataSourceDescriptor dataSource = MRInput
				.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, "artikel.csv")
				.groupSplits(false).generateSplitsInAM(false).build();

		// Knoten erzeugen
		Vertex v1 = Vertex.create(V1, ProcessorDescriptor.create(TokenProcessor.class.getName()));
		v1.addDataSource(INPUT, dataSource);
		Vertex v2 = Vertex.create(V2, ProcessorDescriptor.create(SumProcessor.class.getName()));
		v2.addDataSink(OUTPUT, dataSink1);

		OrderedPartitionedKVEdgeConfig edgeConf = OrderedPartitionedKVEdgeConfig
				.newBuilder(Text.class.getName(), IntWritable.class.getName(), HashPartitioner.class.getName())
				.setFromConfiguration(tezConf).build();
		Edge e1 = Edge.create(v1, v2, edgeConf.createDefaultEdgeProperty());

		// Graph erzeugen
		DAG dag = DAG.create("dag");
		dag.addVertex(v1).addVertex(v2).addEdge(e1);
		tezClient.submitDAG(dag);

		return 0;
	}
}
