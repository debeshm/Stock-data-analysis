import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import java.io.DataInput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeStockAdvanced
{
    public static class TokenizerMapperAdvanced extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String line=itr.nextToken();
            String [] values=line.split(",");
            try
            {

                //key: (ticker year)      value: (low high)
                //passing as space separated values
                context.write(new Text(values[0] + " " + values[1].split("-")[0]), new Text(values[4] + " " + values[3]));
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public static class AnalysisReducerAdvanced extends Reducer<Text, Text, Text, Text> 
    {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            double max_fluctuation=0, low, high;
            String ticker="", high_r="", low_r="";
            String[] l;

            //get ticker name and year
            String[] ticker_year=key.toString().split(" ");

            for(Text val : values)
            {
                //compare all the fluctuation and record the maximum
                l=val.toString().split(" ");
                low=Double.parseDouble(l[0]);
                high=Double.parseDouble(l[1]);
                if(high-low>max_fluctuation)
                {
                    max_fluctuation=high-low;
                    low_r=l[0];
                    high_r=l[1];
                }
            }
            System.out.println(ticker_year[1]+" "+ticker_year[0]+" "+low_r+" "+high_r+" "+String.valueOf(max_fluctuation)); //logs
            context.write(key, new Text(low_r+" "+high_r+" "+max_fluctuation));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock analysis advanced");
        job.setJarByClass(AnalyzeStockAdvanced.class);
        job.setMapperClass(TokenizerMapperAdvanced.class);
        //job.setCombinerClass(AnalysisReducerAdvanced.class);
        job.setReducerClass(AnalysisReducerAdvanced.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}