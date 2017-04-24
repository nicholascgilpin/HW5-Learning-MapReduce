import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

/*
The following code was made utilizing the hadoop tutorial code at
https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Job_Submission_and_Monitoring
Which we were instructed to look at by the instructor.
*/

public class MapreduceSleep {

    public static class TokenizerMapper 
        extends Mapper<Object, Text, Text, IntWritable>{
        private Text hour = new Text();
        private final static IntWritable one = new IntWritable(1);
    
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            
            StringTokenizer iterable = new StringTokenizer(value.toString());
            
            if(iterable.hasMoreTokens()){
                if(iterable.nextToken().equals("T")){
                    iterable.nextToken();
                    hour.set(iterable.nextToken().substring(0, 2));

                    while (iterable.hasMoreTokens()) {
                        if(iterable.nextToken().equals("sleep")){
                            context.write(hour, one);
                        }
                    }
                }
            }
        }
    }

    public static class IntSumReducer 
        extends Reducer<Text,IntWritable,Text,IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
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
        Job job = Job.getInstance(conf, "sleep count");
        conf.setInt(NLineInputFormat.LINES_PER_MAP, 4);
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 4);
        NLineInputFormat.addInputPath(job, new Path(args[0]));
        job.setJarByClass(MapreduceSleep.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
