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
import org.apache.hadoop.mapred.TextInputFormat;

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
            
            StringTokenizer iterable = new StringTokenizer(value.toString()); //For getting the time
            String line = value.toString().toLowerCase(); //For getting the line potentially containing sleep
            
            if(iterable.hasMoreTokens() || line != null){ //If the string token has T or is not null, go in
                if(iterable.nextToken().equals("T")){ //If it has T, grab the two tokens for the hours from 00-23
                    iterable.nextToken();
                    hour.set(iterable.nextToken().substring(0, 2));
                }
                if(line.contains("sleep")){ //If the value passed in contains sleep, increment.
                    context.write(hour, one);
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
        conf.set("textinputformat.record.delimiter","\n\n");
        // Solution found in the comment at:https://hadoopi.wordpress.com/2013/05/27/understand-recordreader-inputsplit/
        //Takes advantage of the empty line between tweets
        
        Job job = Job.getInstance(conf, "sleep count");
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 4); //Sets the conf as the lines in each tweet  = 4     
        job.setJarByClass(MapreduceSleep.class);
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
