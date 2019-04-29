import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnalyzeStock
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>
    {
        private static String field;
        public static Date startDate;
        public static Date endDate;
        private Text companyName = new Text();

        protected void setup(Mapper.Context context) throws IOException, InterruptedException
        {
            Configuration config = context.getConfiguration();
            field=config.get("field");
            String [] startDate_attrs=config.get("start_date").split("/");
            startDate=new Date(Integer.parseInt(startDate_attrs[2]), Integer.parseInt(startDate_attrs[0]), Integer.parseInt(startDate_attrs[1]));
            String [] endDate_atrrs=config.get("end_date").split("/");
            endDate=new Date(Integer.parseInt(endDate_atrrs[2]), Integer.parseInt(endDate_atrrs[0]), Integer.parseInt(endDate_atrrs[1]));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String line=itr.nextToken();
            String [] values=line.split(",");
            try
            {
                companyName.set(values[0]);
                String [] date_attrs = values[1].split("-");
                Date currentDate=new Date(Integer.parseInt(date_attrs[0]), Integer.parseInt(date_attrs[1]), Integer.parseInt(date_attrs[2]));

                if(currentDate.compareTo(endDate)==0 || currentDate.compareTo(startDate)==0 || (currentDate.after(startDate) && currentDate.before(endDate)))
                {
                    if(field.equals("close"))
                    {
                        FloatWritable closingValue=new FloatWritable(Float.parseFloat(values[5]));
                        context.write(companyName, closingValue);
                    }
                    else if(field.equals("low"))
                    {
                        FloatWritable low=new FloatWritable(Float.parseFloat(values[4]));
                        context.write(companyName, low);
                    }
                    else if(field.equals("high"))
                    {
                        FloatWritable high=new FloatWritable(Float.parseFloat(values[3]));
                        context.write(companyName, high);
                    }
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    public static class AnalysisReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> 
    {
        private FloatWritable result = new FloatWritable();
        private String operation;

        protected void setup(Reducer.Context context) throws IOException, InterruptedException
        {
            Configuration config = context.getConfiguration();
            operation=config.get("operation");
        }

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException 
        {
            float res=0;
            if(operation.equals("avg"))
            {
                float sum=0;
                int c=0;
                //talke sum and divide by total number of data for average
                for (FloatWritable val : values) 
                {
                    sum += val.get();
                    c+=1;
                }
                res=sum/c;
            }
            else if(operation.equals("min"))
            {
                res=10000;
                //iterate and find min
                for (FloatWritable val : values) 
                {
                    res=res<val.get()?res:val.get();
                }
            }
            else if(operation.equals("max"))
            {
                //iterate and find max
                for (FloatWritable val : values) 
                {
                    res=res>val.get()?res:val.get();
                }
            }
            res = (float) (Math.round(res * 10) / 10.0);
            result.set(res);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        conf.set("start_date", args[0]);
        conf.set("end_date", args[1]);
        conf.set("operation", args[2]);
        conf.set("field", args[3]);
        Job job = Job.getInstance(conf, "Stock analysis");
        job.setJarByClass(AnalyzeStock.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(AnalysisReducer.class);
        job.setReducerClass(AnalysisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        job.setNumReduceTasks(4);
        FileInputFormat.addInputPath(job, new Path(args[4]));
        FileOutputFormat.setOutputPath(job, new Path(args[5]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}