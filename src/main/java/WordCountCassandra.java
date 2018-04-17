import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountCassandra {

    public static class TokenizerMapper
            extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, Context context
        ) throws IOException, InterruptedException {
            String foo = ByteBufferUtil.string(columns.get("foo"));
            StringTokenizer itr = new StringTokenizer(foo);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count cassandra");
        job.setJarByClass(WordCountCassandra.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "206.189.16.183");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), "nyao", "simple");
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");
        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
        job.setInputFormatClass(CqlPagingInputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
