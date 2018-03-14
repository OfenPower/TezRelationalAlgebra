package de.bachd.bigdata.tez;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class DAGBuilder {

	private boolean needSelection = false;

	private UserPayload projectionParameter;
	private UserPayload selectionParameter;
	private UserPayload tableParameter;

	private List<String> dataSourceList = new ArrayList<>();
	private List<DataSourceDescriptor> dataSourceDescriptorList = new ArrayList<>();
	private DataSinkDescriptor outputTable;
	private DataSinkDescriptor outputScheme;

	private int vertexCount = 0;
	private List<Vertex> vertexList = new ArrayList<>();
	private List<Edge> edgeList = new ArrayList<>();

	public DAG buildDAGFromString(String query, TezConfiguration tezConf) throws TezException, IOException {
		initializeUserPayloads(query);
		initializeDataSourcesAndDataSinks(tezConf);
		if (needSelection)
			initializeSelectionVertex(tezConf);
		initializeProjectionVertex(tezConf);

		// DAG zusammenbasteln
		DAG dag = DAG.create("DAG");
		for (Vertex v : vertexList) {
			dag.addVertex(v);
		}
		for (Edge e : edgeList) {
			dag.addEdge(e);
		}

		return dag;
	}

	private void initializeUserPayloads(String query) throws TezException, IOException {

		List<String> queryParts = Lists.newArrayList(Splitter.on("\n").trimResults().omitEmptyStrings().split(query));
		for (String s : queryParts) {
			if (s.startsWith("SELECT")) {
				setProjectionAndAggregationPayload(s);
			}
			if (s.startsWith("FROM")) {
				setTablePayload(s);
			}
			if (s.startsWith("WHERE")) {
				setSelectionPayload(s);
			}
		}
	}

	private void initializeDataSourcesAndDataSinks(TezConfiguration tezConf) {
		// Für jede gespeicherte Relation in dataSourceList je zwei
		// DataSourceDescriptor erzeugen (1x für .csv und 1x für .scheme)
		for (String dataSource : dataSourceList) {
			DataSourceDescriptor data = MRInput
					.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, "./tables/" + dataSource)
					.build();
			DataSourceDescriptor scheme = MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class,
					"./tables/" + dataSource + ".scheme").build();
			dataSourceDescriptorList.add(data);
			dataSourceDescriptorList.add(scheme);

			// Je zwei Vertices erzugen (1x scheme, 1x data), diese mit
			// vertexCount durchnummerieren (v1, v2, etc.) und in Liste
			// abspeichern
			Vertex v1 = Vertex.create("v" + ++vertexCount, ProcessorDescriptor.create(SchemeProcessor.class.getName()));
			Vertex v2 = Vertex.create("v" + ++vertexCount, ProcessorDescriptor.create(TableProcessor.class.getName()));
			v1.addDataSource("SchemeInput", scheme);
			v2.addDataSource("DataInput", data);
			vertexList.add(v1);
			vertexList.add(v2);

			// Die erzeugten Vertices mit einer Edge verbinden und die Edge
			// abspeichern
			UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig.newBuilder(Text.class.getName(), Text.class.getName())
					.setFromConfiguration(tezConf).build();
			Edge e1 = Edge.create(v1, v2, eConfig.createDefaultBroadcastEdgeProperty());
			edgeList.add(e1);
		}

		// Zwei DataSinks erzeugen (1x für Ergebnisdaten und 1x für das
		// Ergebnisschema)
		outputTable = MROutput
				.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, "./tables/output_table")
				.build();
		outputScheme = MROutput
				.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, "./tables/output_scheme")
				.build();
	}

	private void initializeSelectionVertex(TezConfiguration tezConf) {
		// Selektionsknoten erzeugen und speichern
		Vertex v = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(SelectionProcessor.class.getName()).setUserPayload(selectionParameter), 1);
		vertexList.add(v);

		// Vorletzten Vertex der Liste holen und mit Selektionsknoten über Edge
		// verknüpfen
		UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig
				.newBuilder(Tuple.class.getName(), NullWritable.class.getName()).setFromConfiguration(tezConf).build();
		Vertex vertexToConnect = vertexList.get(vertexList.size() - 2);
		Edge e = Edge.create(vertexToConnect, v, eConfig.createDefaultBroadcastEdgeProperty());

		edgeList.add(e);
	}

	private void initializeProjectionVertex(TezConfiguration tezConf) {
		// Projektionsknoten erzeugen, speichern und mit DataSinks verknüpfen
		Vertex v = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(ProjectionProcessor.class.getName()).setUserPayload(projectionParameter), 1);
		v.addDataSink("OutputTable", outputTable);
		v.addDataSink("OutputScheme", outputScheme);
		vertexList.add(v);

		// Vorletzten Vertex der Liste holen und mit Projektionsknoten über Edge
		// verknüpfen
		UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig
				.newBuilder(Tuple.class.getName(), NullWritable.class.getName()).setFromConfiguration(tezConf).build();
		Vertex vertexToConnect = vertexList.get(vertexList.size() - 2);
		Edge e = Edge.create(vertexToConnect, v, eConfig.createDefaultBroadcastEdgeProperty());
		edgeList.add(e);
	}

	private void setProjectionAndAggregationPayload(String queryPart) throws IOException {
		System.out.println("---- setProjectionAndAggregationConfiguration ----");
		// SELECT-wort abschneiden
		String projectionString = queryPart.substring(7);
		System.out.println(projectionString);
		Configuration conf = new Configuration();
		conf.set("Projection", projectionString);
		this.projectionParameter = TezUtils.createUserPayloadFromConf(conf);
	}

	private void setTablePayload(String queryPart) throws IOException {
		System.out.println("---- setTableConfiguration ----");
		List<String> queryParts = Lists
				.newArrayList(Splitter.on(' ').trimResults().omitEmptyStrings().split(queryPart));
		for (String s : queryParts) {
			System.out.print(s + " ");
		}
		System.out.println();
		// Name der Source-Relation speichern
		this.dataSourceList.add(queryParts.get(1));
		// Muss eine Relation umbenannt werden?
		Configuration conf = new Configuration();
		if (queryParts.size() == 3) {
			// queryPart hat folgende Form: FROM <table> <newTableName>
			conf.set("NewRelationRename", queryParts.get(2));
		} else {
			// queryPart hat folgende Form: FROM <table>
			// => Keine Umbennenung nötig
			conf.set("NewRelationRename", "");
		}
		this.tableParameter = TezUtils.createUserPayloadFromConf(conf);
	}

	private void setSelectionPayload(String queryPart) throws IOException {
		System.out.println("---- setSelectionConfiguration ----");
		// WHERE-wort abschneiden
		String selectionString = queryPart.substring(6);
		System.out.println(selectionString);
		Configuration conf = new Configuration();
		conf.set("SelectionPredicate", selectionString);
		this.selectionParameter = TezUtils.createUserPayloadFromConf(conf);
		needSelection = true; // flag setzen, um später einen Selection-Vertex
								// zu erzeugen
	}

	/*
	 * private void setGroupByConfiguration(String queryPart) {
	 * System.out.println("---- setGroupByConfiguration ----"); List<String>
	 * queryParts = Lists .newArrayList(Splitter.on('
	 * ').trimResults().omitEmptyStrings().limit(3).split(queryPart));
	 * System.out.println(queryParts.get(2));
	 * this.groupByConfiguration.set("GroupByAttributes", queryParts.get(2)); }
	 */

	private void aggregate() {
		// AggregationConfiguration
		//
		List<String> queryParts02 = Lists.newArrayList(Splitter.on(CharMatcher.anyOf(" ").or(CharMatcher.anyOf(", ")))
				.trimResults().omitEmptyStrings().split("queryPart"));
		// Aggregatfunktionen im SELECT-Statement erkennt man an einer öffnenden
		// Klammer!
		StringBuilder sb = new StringBuilder();
		for (int i = 1; i < queryParts02.size(); i++) {
			if (queryParts02.get(i).contains("(")) {
				sb.append(queryParts02.get(i));
				sb.append(", ");
			}
		}
		sb.delete(sb.length() - 2, sb.length());
		System.out.println(sb.toString());
		// this.aggregationParameter.set("AggregateFunctions", sb.toString());
	}
}