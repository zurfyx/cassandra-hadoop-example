package main;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.BasicConfigurator;

public class WordCountCassandra {

    public static class TokenizerMapper
            extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, Context context
        ) throws IOException, InterruptedException {
            ByteBuffer agentBytes = columns.get("agent");
            String agent = agentBytes == null ? "-" : ByteBufferUtil.string(agentBytes);
            word.set(agent);
            context.write(word, one);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
            keys.put("name", ByteBufferUtil.bytes(key.toString()));

            List<ByteBuffer> variables = new ArrayList<ByteBuffer>();
            variables.add(ByteBufferUtil.bytes(sum));
            context.write(keys, variables);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count cassandra");
        job.setJarByClass(WordCountCassandra.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "206.189.16.183");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), "nyao", "visitors");
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "200");
        job.setInputFormatClass(CqlPagingInputFormat.class);

        job.setOutputFormatClass(CqlOutputFormat.class);
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), "nyao", "count");
        String query = "UPDATE count SET msg = ?";
        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), "206.189.16.183");
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
