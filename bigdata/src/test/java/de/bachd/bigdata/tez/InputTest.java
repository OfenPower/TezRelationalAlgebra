package de.bachd.bigdata.tez;

import java.util.List;

import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;

class WriterTest extends Writer {

}

public class InputTest extends AbstractLogicalInput {

	public InputTest(InputContext inputContext, int numPhysicalInputs) {
		super(inputContext, numPhysicalInputs);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public Reader getReader() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void handleEvents(List<Event> inputEvents) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public List<Event> close() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Event> initialize() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
