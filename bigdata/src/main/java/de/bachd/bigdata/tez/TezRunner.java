package de.bachd.bigdata.tez;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;

public class TezRunner {

	public int run(String[] args) throws Exception {
		// Tez lokal aufsetzen
		TezConfiguration tezConf = new TezConfiguration();
		tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
		tezConf.set("fs.default.name", "file:///");
		tezConf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

		// Query aus angegebener Datei in einen String auslesen
		String query = readQueryFromFile(args[0]);

		// DAG aus String erzeugen
		DAGBuilder dagBuilder = new DAGBuilder();
		DAG dag = dagBuilder.buildDAGFromString(query, tezConf);

		// TezClient starten
		TezClient tezClient = TezClient.create("TezClient", tezConf);
		tezClient.start();

		return runDAGJob(dag, tezConf, tezClient);
	}

	private int runDAGJob(DAG dag, TezConfiguration tezConf, TezClient tezClient) throws TezException, IOException {
		System.out.println("Start DAG!");
		try {
			DAGClient dagClient = tezClient.submitDAG(dag);
			DAGStatus dagStatus;
			dagStatus = dagClient.waitForCompletionWithStatusUpdates(null);
			System.out.println("DAG diagnostics: " + dagStatus.getDiagnostics());
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}

		return 0;
	}

	private String readQueryFromFile(String fileName) throws FileNotFoundException, IOException {
		StringBuilder sb = new StringBuilder();
		String fullFileName = "./querys/" + fileName;
		try (FileReader fin = new FileReader(new File(fullFileName)); BufferedReader br = new BufferedReader(fin)) {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line + "\n");
			}
		}
		return sb.toString();
	}

}
