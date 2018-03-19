package de.bachd.bigdata.tez;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.tez.runtime.library.conf.OrderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedKVEdgeConfig;
import org.apache.tez.runtime.library.conf.UnorderedPartitionedKVEdgeConfig;
import org.apache.tez.runtime.library.partitioner.HashPartitioner;

public class EdgeConfigs {

	UnorderedKVEdgeConfig eConf1 = UnorderedKVEdgeConfig.newBuilder(IntWritable.class.getName(), Text.class.getName())
			.build();
	UnorderedPartitionedKVEdgeConfig eConf2 = UnorderedPartitionedKVEdgeConfig
			.newBuilder(IntWritable.class.getName(), Text.class.getName(), HashPartitioner.class.getName()).build();
	OrderedPartitionedKVEdgeConfig eConf3 = OrderedPartitionedKVEdgeConfig
			.newBuilder(IntWritable.class.getName(), Text.class.getName(), HashPartitioner.class.getName()).build();
}
