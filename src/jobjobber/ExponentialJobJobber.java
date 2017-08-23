package jobjobber;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.util.Random;


public class ExponentialJobJobber {

    public static class JobJobberMapper
    extends Mapper<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {

        public static Random random = new Random();

        public void map(LongWritable taskId, DoubleWritable t, Context context) throws IOException, InterruptedException {

            System.out.println("    +++ TASK "+taskId+" duration: "+t.get());
            long startTime = java.lang.System.currentTimeMillis();
            long targetStopTime = startTime + (long)(1000*t.get());
            System.out.println("    +++ TASK "+taskId+" START: "+startTime);
            while (java.lang.System.currentTimeMillis() < targetStopTime) {
                double x = random.nextDouble() * 2 - 1;
                double y = random.nextDouble() * 2 - 1;
            }
            
            long stopTime = java.lang.System.currentTimeMillis();
            System.out.println("    +++ TASK "+taskId+" STOP: "+stopTime);
            context.write(new LongWritable(taskId.get()), new DoubleWritable(t.get()));
            System.out.flush();
        }
    }
    
    
    public static class JobJobberReducer
    extends Reducer<LongWritable,DoubleWritable,LongWritable,DoubleWritable> {

        public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("REDUCERREDUCERREDUCER");
            for (DoubleWritable val : values) {
                context.write(key, val);
                System.out.println("REDUCER: "+key+"\t"+val);
            }
            //result.set(sum);
            //context.write(new Text(key.toString()), result);
            System.out.flush();
        }
    }
    

    /**
     * main routine
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int num_jobs = 1;
        double arrival_rate = 0.2;
        Random rng = new Random();
        long app_start_time = System.currentTimeMillis();
        
        for (int i=0; i<num_jobs; i++) {
        
            Job job = Job.getInstance(conf, "job_jobber_"+i);
            job.setJarByClass(ExponentialJobJobber.class);
            job.setInputFormatClass(JobJobberInputFormat.class);
            job.setMapperClass(JobJobberMapper.class);
            job.setCombinerClass(JobJobberReducer.class);
            //job.setReducerClass(IntSumReducer.class);
            job.setReducerClass(JobJobberReducer.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(DoubleWritable.class);
            //FileInputFormat.addInputPath(job, new Path("/user/ikt/input"));
            System.out.println("setOutputPath("+i+")");
            FileOutputFormat.setOutputPath(job, new Path("/user/ikt/junk"+app_start_time+"/"+i));
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
  
            // on the last pass, submit the job and wait for it
            //if (i < (num_jobs-1)) {
                job.submit();
            //} else {
            //    job.waitForCompletion(true);
            //}
            
            double sleeptime = -Math.log(rng.nextDouble())/arrival_rate;
            Thread.sleep((long)(sleeptime * 1000));
        }
    }
    
}
