package de.bachd.bigdata.tez;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class DAGBuilder {

	private boolean needProjection = false;
	private boolean needSelection = false;
	private boolean needGroup = false;

	private UserPayload projectionParameter;
	private UserPayload selectionParameter;
	private UserPayload groupByParameter;
	private UserPayload aggregationParameter;
	private UserPayload tableParameter;

	private ArrayDeque<String> dataSourceQueue = new ArrayDeque<>();

	private int vertexCount = 0;
	private ArrayDeque<Vertex> vertexQueue = new ArrayDeque<>();
	private ArrayDeque<Edge> edgeQueue = new ArrayDeque<>();
	private Map<String, Vertex> relationVertexMap = new HashMap<>();
	private Map<String, String> relationJoinattributeMap = new HashMap<>();

	public DAG buildDAGFromString(String query, TezConfiguration tezConf) throws Exception {
		initializeUserPayloads(query);
		initializeSchemeAndTableProcessorVertices(tezConf);
		// Wenn mehr als eine DataSource vorhanden: Ersten Joinknoten
		// initialisieren
		if (dataSourceQueue.size() >= 2) {
			initializeFirstJoinVertex(tezConf);
			// Solange noch DataSources vorhanden sind => zusätzliche Joinknoten
			// initialisieren und anfügen
			while (!dataSourceQueue.isEmpty()) {
				initializeJoinVertex(tezConf);
			}
		}

		// Selektion benötigt?
		if (needSelection) {
			initializeSelectionVertex(tezConf);
		}
		// Gruppierung benötigt?
		if (needGroup) {
			initializeGroupByAndAggregationVertex(tezConf);
		}
		// Projektion benötigt?
		if (needProjection) {
			initializeProjectionVertex(tezConf);
		}
		initializeOutputVertex(tezConf);

		// DAG zusammenbasteln
		DAG dag = DAG.create("DAG");
		for (Vertex v : vertexQueue) {
			dag.addVertex(v);
		}
		for (Edge e : edgeQueue) {
			dag.addEdge(e);
		}

		return dag;
	}

	private void initializeUserPayloads(String query) throws TezException, IOException {
		List<String> queryParts = Lists.newArrayList(Splitter.on("\n").trimResults().omitEmptyStrings().split(query));
		for (String s : queryParts) {
			// Projektionspayload initialisieren
			if (s.startsWith("SELECT")) {
				setProjectionPayload(s.substring(7));
				setAggregationPayload(s.substring(7));
			}
			// Erste DataSource initialisieren
			if (s.startsWith("FROM")) {
				setTablePayload(s.substring(5));
			}
			// Weitere DataSources pro JOINs initialisieren
			if (s.startsWith("JOIN")) {
				prepareJoinInformation(s.substring(5));
			}
			// Selektionspayload initialisieren
			if (s.startsWith("WHERE")) {
				setSelectionPayload(s.substring(6));
			}
			// GroupByPayload initialisieren
			if (s.startsWith("GROUP BY")) {
				setGroupByPayload(s.substring(9));
			}
		}
	}

	private void initializeSchemeAndTableProcessorVertices(TezConfiguration tezConf) {
		// Für jede gespeicherte Relation in dataSourceList je zwei
		// DataSourceDescriptor erzeugen (1x für .csv und 1x für .scheme)
		for (String dataSource : dataSourceQueue) {
			DataSourceDescriptor data = MRInput
					.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class, "./tables/" + dataSource)
					.build();
			DataSourceDescriptor scheme = MRInput.createConfigBuilder(new Configuration(tezConf), TextInputFormat.class,
					"./tables/" + dataSource + ".scheme").build();

			// Je zwei Vertices erzugen (1x scheme, 1x data), diese mit
			// vertexCount durchnummerieren (v1, v2, etc.) und in Liste
			// abspeichern
			Vertex v1 = Vertex.create("v" + ++vertexCount, ProcessorDescriptor.create(SchemeProcessor.class.getName()));
			Vertex v2 = Vertex.create("v" + ++vertexCount, ProcessorDescriptor.create(TableProcessor.class.getName()));
			v1.addDataSource("SchemeInput", scheme);
			v2.addDataSource("DataInput", data);
			vertexQueue.add(v1);
			vertexQueue.add(v2);
			// System.out.println("Parallelism: " + v2.getParallelism());

			// Die erzeugten Vertices mit einer Edge verbinden und die Edge
			// abspeichern
			UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig.newBuilder(Text.class.getName(), Text.class.getName())
					.setFromConfiguration(tezConf).build();
			// Edge e1 = Edge.create(v1, v2,
			// eConfig.createDefaultBroadcastEdgeProperty());
			Edge e1 = Edge.create(v1, v2, eConfig.createDefaultBroadcastEdgeProperty());
			edgeQueue.add(e1);

			// relation mit dem Namen des Vertex welcher diese Relation im
			// TableProcessor lädt in einer Map speichern (Bsp: <artikel, v2>,
			// <lager, v4>))
			relationVertexMap.put(dataSource, v2);
			// System.out.println("RelationVertex Eintrag: " + dataSource + "
			// verarbeitet von Vertex: " + v2.getName());
		}

	}

	private void initializeFirstJoinVertex(TezConfiguration tezConf) throws Exception {
		// Die ersten beiden zu joinenden Relation aus der DataSourceQueue
		// entnehmen
		String leftRelationToJoin = dataSourceQueue.removeFirst();
		String rightRelationToJoin = dataSourceQueue.removeFirst();

		// Joinattribut (wurde bei zweiter Relation abgespeichert) aus Map
		// nehmen
		String joinAttribute = relationJoinattributeMap.get(rightRelationToJoin);

		// Namen der Vertices holen, welche die jeweilige Relation im
		// TableProcessor vorbereiten
		Vertex leftJoinVertex = relationVertexMap.get(leftRelationToJoin);
		Vertex rightJoinVertex = relationVertexMap.get(rightRelationToJoin);
		String leftRelationVertexName = leftJoinVertex.getName();
		String rightRelationVertexName = rightJoinVertex.getName();

		// UserPayload mit Joinparametern initialisieren
		Configuration conf = new Configuration();
		if (joinAttribute.startsWith(leftRelationToJoin)) {
			conf.set("LeftJoinVertexName", leftRelationVertexName);
			conf.set("RightJoinVertexName", rightRelationVertexName);
		} else {
			conf.set("LeftJoinVertexName", rightRelationVertexName);
			conf.set("RightJoinVertexName", leftRelationVertexName);
		}
		conf.set("JoinAttribute", joinAttribute);
		UserPayload up = TezUtils.createUserPayloadFromConf(conf);

		// HashJoin-Vertex erzeugen
		Vertex hashJoinVertex = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(HashJoinProcessor.class.getName()).setUserPayload(up));

		// Join-Vertex mit zwei Edges an die jeweiligen Vertices koppeln, welche
		// gejoined werden sollen
		UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig
				.newBuilder(NullWritable.class.getName(), Tuple.class.getName()).setFromConfiguration(tezConf).build();
		// Edge e1 = Edge.create(leftJoinVertex, hashJoinVertex,
		// eConfig.createDefaultBroadcastEdgeProperty());
		// Edge e2 = Edge.create(rightJoinVertex, hashJoinVertex,
		// eConfig.createDefaultBroadcastEdgeProperty());
		Edge e1 = Edge.create(leftJoinVertex, hashJoinVertex, eConfig.createDefaultOneToOneEdgeProperty());
		Edge e2 = Edge.create(rightJoinVertex, hashJoinVertex, eConfig.createDefaultOneToOneEdgeProperty());

		// Vertex und zwei Edges in die jeweiligen Queues stecken
		vertexQueue.add(hashJoinVertex);
		edgeQueue.add(e1);
		edgeQueue.add(e2);
	}

	private void initializeJoinVertex(TezConfiguration tezConf) throws IOException {
		// Zu joinende Relation aus der dataSourceQueue nehmen
		String relationToJoin = dataSourceQueue.removeFirst();

		// dazugehöriges Joinattribut aus Map nehmen
		String joinAttribute = relationJoinattributeMap.get(relationToJoin);

		// Zu joinende Knoten holen
		Vertex vertexToJoin = relationVertexMap.get(relationToJoin);
		Vertex existingJoinVertex = vertexQueue.getLast();
		String relationVertexName = vertexToJoin.getName();
		String hashJoinVertexName = existingJoinVertex.getName();

		// UserPayload mit Joinparametern initialisieren
		Configuration conf = new Configuration();
		if (joinAttribute.startsWith(relationToJoin)) {
			conf.set("LeftJoinVertexName", relationVertexName);
			conf.set("RightJoinVertexName", hashJoinVertexName);
		} else {
			conf.set("LeftJoinVertexName", hashJoinVertexName);
			conf.set("RightJoinVertexName", relationVertexName);
		}
		conf.set("JoinAttribute", joinAttribute);
		UserPayload up = TezUtils.createUserPayloadFromConf(conf);

		// HashJoin-Vertex erzeugen
		Vertex newHashJoinVertex = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(HashJoinProcessor.class.getName()).setUserPayload(up));

		// Join-Vertex mit zwei Edges an die jeweiligen Vertices koppeln, welche
		// gejoined werden sollen
		UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig
				.newBuilder(NullWritable.class.getName(), Tuple.class.getName()).setFromConfiguration(tezConf).build();
		Edge e1 = Edge.create(existingJoinVertex, newHashJoinVertex, eConfig.createDefaultOneToOneEdgeProperty());
		Edge e2 = Edge.create(vertexToJoin, newHashJoinVertex, eConfig.createDefaultOneToOneEdgeProperty());

		// Vertex und zwei Edges in die jeweiligen Queues stecken
		vertexQueue.add(newHashJoinVertex);
		edgeQueue.add(e1);
		edgeQueue.add(e2);
	}

	private void initializeSelectionVertex(TezConfiguration tezConf) {
		// Selektionsknoten erzeugen und speichern
		Vertex v = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(SelectionProcessor.class.getName()).setUserPayload(selectionParameter));
		Vertex vertexToConnect = vertexQueue.getLast();
		vertexQueue.add(v);

		// Vorletzten Vertex der Liste holen und mit Selektionsknoten über Edge
		// verknüpfen
		UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig
				.newBuilder(NullWritable.class.getName(), Tuple.class.getName()).setFromConfiguration(tezConf).build();
		// Vertex vertexToConnect = vertexList.get(vertexList.size() - 2);
		// Edge e = Edge.create(vertexToConnect, v,
		// eConfig.createDefaultBroadcastEdgeProperty());
		Edge e = Edge.create(vertexToConnect, v, eConfig.createDefaultOneToOneEdgeProperty());
		edgeQueue.add(e);
	}

	private void initializeGroupByAndAggregationVertex(TezConfiguration tezConf) throws IOException {
		// GroupByVertex erzeugen und mit letztem Knoten verbinden
		Vertex groupByVertex = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(GroupByProcessor.class.getName()).setUserPayload(groupByParameter));
		Vertex vertexToConnect = vertexQueue.getLast();
		vertexQueue.add(groupByVertex);

		UnorderedKVEdgeConfig eConfig1 = UnorderedKVEdgeConfig
				.newBuilder(NullWritable.class.getName(), Tuple.class.getName()).setFromConfiguration(tezConf).build();
		Edge e1 = Edge.create(vertexToConnect, groupByVertex, eConfig1.createDefaultOneToOneEdgeProperty());
		edgeQueue.add(e1);

		// Aggregationsvertex erzeugen und mit GroupBy-Knoten verbinden
		Vertex aggregateVertex = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(AggregationProcessor.class.getName()).setUserPayload(aggregationParameter),
				1);
		vertexQueue.add(aggregateVertex);

		OrderedPartitionedKVEdgeConfig eConfig2 = OrderedPartitionedKVEdgeConfig
				.newBuilder(Text.class.getName(), Tuple.class.getName(), HashPartitioner.class.getName()).build();
		Edge e2 = Edge.create(groupByVertex, aggregateVertex, eConfig2.createDefaultEdgeProperty());
		edgeQueue.add(e2);
	}

	private void initializeProjectionVertex(TezConfiguration tezConf) {
		// Projektionsknoten erzeugen, speichern und mit DataSinks verknüpfen
		Vertex v = Vertex.create("v" + ++vertexCount,
				ProcessorDescriptor.create(ProjectionProcessor.class.getName()).setUserPayload(projectionParameter));
		Vertex vertexToConnect = vertexQueue.getLast();
		vertexQueue.add(v);

		// Vorletzten Vertex der Liste holen und mit Projektionsknoten über Edge
		// verknüpfen
		UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig
				.newBuilder(NullWritable.class.getName(), Tuple.class.getName()).setFromConfiguration(tezConf).build();
		Edge e = Edge.create(vertexToConnect, v, eConfig.createDefaultOneToOneEdgeProperty());
		edgeQueue.add(e);
	}

	private void initializeOutputVertex(TezConfiguration tezConf) {
		// Zwei DataSinks erzeugen (1x für Ergebnisdaten und 1x für das
		// Ergebnisschema)
		DataSinkDescriptor outputTable = MROutput
				.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, "./output_table").build();
		DataSinkDescriptor outputScheme = MROutput
				.createConfigBuilder(new Configuration(tezConf), TextOutputFormat.class, "./output_scheme").build();

		// Projektionsknoten erzeugen, speichern und mit DataSinks verknüpfen
		Vertex v = Vertex.create("v" + ++vertexCount, ProcessorDescriptor.create(OutputProcessor.class.getName()));
		v.addDataSink("OutputTable", outputTable);
		v.addDataSink("OutputScheme", outputScheme);
		Vertex vertexToConnect = vertexQueue.getLast();
		vertexQueue.add(v);

		// Vorletzten Vertex der Liste holen und mit Projektionsknoten über Edge
		// verknüpfen
		UnorderedKVEdgeConfig eConfig = UnorderedKVEdgeConfig
				.newBuilder(NullWritable.class.getName(), Tuple.class.getName()).setFromConfiguration(tezConf).build();
		Edge e = Edge.create(vertexToConnect, v, eConfig.createDefaultOneToOneEdgeProperty());
		edgeQueue.add(e);
	}

	private void setProjectionPayload(String queryPart) throws IOException {
		// Muss projeziert werden?
		if (!queryPart.trim().contains("*")) {
			needProjection = true;
			Configuration conf = new Configuration();
			conf.set("Projection", queryPart);
			this.projectionParameter = TezUtils.createUserPayloadFromConf(conf);
		}
	}

	private void setAggregationPayload(String queryPart) throws IOException {
		// Aggregatfunktionen in Configuration speichern, falls vorhanden
		List<String> queryParts = Lists
				.newArrayList(Splitter.on(",").trimResults().omitEmptyStrings().split(queryPart));
		// Aggregatfunktionen im SELECT-Statement erkennt man an einer öffnenden
		// Klammer!
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < queryParts.size(); i++) {
			if (queryParts.get(i).contains("(")) {
				sb.append(queryParts.get(i));
				sb.append(", ");
			}
		}
		Configuration conf = new Configuration();
		conf.set("AggregateFunctions", sb.toString());
		this.aggregationParameter = TezUtils.createUserPayloadFromConf(conf);
	}

	private void setTablePayload(String queryPart) throws IOException {
		// Name der Source-Relation speichern
		this.dataSourceQueue.add(queryPart);

		// ToDo: Eventuelle Payload für TableProcessor hier erzeugen!
	}

	private void setSelectionPayload(String queryPart) throws IOException {
		// System.out.println(selectionString);
		Configuration conf = new Configuration();
		conf.set("SelectionPredicate", queryPart);
		this.selectionParameter = TezUtils.createUserPayloadFromConf(conf);
		needSelection = true; // flag setzen, um später einen Selection-Vertex
								// zu erzeugen
	}

	private void setGroupByPayload(String queryPart) throws IOException {
		Configuration conf = new Configuration();
		conf.set("GroupByAttributes", queryPart);
		this.groupByParameter = TezUtils.createUserPayloadFromConf(conf);
		needGroup = true; // flag setzen, um später einen GroupBy und
							// Aggregations-Vertex zu erzeugen
	}

	private void prepareJoinInformation(String queryPart) throws IOException {
		// System.out.println("---- setJoinConfiguration ----");
		List<String> queryParts = Lists
				.newArrayList(Splitter.on(' ').trimResults().omitEmptyStrings().limit(3).split(queryPart));
		// for (String s : queryParts) {
		// System.out.println(s);
		// }

		// Neue Relation soll gejoined werden => in dataSourceQueue packen und
		// das dazugehörige Joinattribut speichern
		dataSourceQueue.add(queryParts.get(0));
		relationJoinattributeMap.put(queryParts.get(0), queryParts.get(2));
		// System.out.println(
		// "Zu joinende Relation: " + queryParts.get(0) + " mit folgendem
		// Joinattribut: " + queryParts.get(2));
	}
}
