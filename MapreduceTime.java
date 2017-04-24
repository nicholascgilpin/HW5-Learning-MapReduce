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

/*
The following code was made utilizing the hadoop tutorial code at
https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Job_Submission_and_Monitoring
Which we were instructed to look at by the instructor.
*/

public class MapreduceTime {

    public static class TokenizerMapper 
        extends Mapper<Object, Text, Text, IntWritable>{
        private Text hour = new Text();
        private final static IntWritable one = new IntWritable(1);
       
        public void map(Object key, Text value, Context context
                       ) throws IOException, InterruptedException {
            StringTokenizer iterable = new StringTokenizer(value.toString());
            
            if(iterable.hasMoreTokens()){ //If the string token has tokens, continue
                if(iterable.nextToken().equals("T")){ //If the token is equal to T,
                    iterable.nextToken();
                    hour.set(iterable.nextToken().substring(0, 2)); //Grab the two tokens that correspond to hours from 00-23
                    context.write(hour, one); //map it
                }
            }
        }
    }

      public static class IntSumReducer
           extends Reducer<Text,IntWritable,Text,IntWritable> {
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
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(MapreduceTime.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
