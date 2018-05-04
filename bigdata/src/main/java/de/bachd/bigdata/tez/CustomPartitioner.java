package de.bachd.bigdata.tez;

import org.apache.tez.runtime.library.api.Partitioner;

public class CustomPartitioner implements Partitioner {

	@Override
	public int getPartition(Object key, Object value, int numPartitions) {
		int partition = (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		return partition;
	}

}
