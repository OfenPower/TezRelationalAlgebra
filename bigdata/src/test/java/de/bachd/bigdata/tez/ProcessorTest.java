package de.bachd.bigdata.tez;

import java.util.List;
import java.util.Map;

import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

public class ProcessorTest extends AbstractLogicalIOProcessor {

	public ProcessorTest(ProcessorContext context) {
		super(context);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void handleEvents(List<Event> processorEvents) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void initialize() throws Exception {
		// TODO Auto-generated method stub

	}

}