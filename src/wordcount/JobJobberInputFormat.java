package wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobConf;
import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;


public class JobJobberInputFormat extends InputFormat<LongWritable, DoubleWritable> {
    
    public static int k = 10;
    
    /*
    @Override
    public RecordReader<LongWritable, DoubleWritable> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2) throws IOException {
        return new ExponentialJobJobberRecordReader(1.0);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numsplits) throws IOException {
        // the actual input splits will be null.  Will that still work?
        return new InputSplit[k];
    }
    */

    @Override
    public RecordReader<LongWritable, DoubleWritable> createRecordReader(
            InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        System.out.println("INPUTFORMAT-CREATERECORDREADER");
        return new ExponentialJobJobberRecordReader(0.025);
    }

    @Override
    public List<InputSplit> getSplits(JobContext arg0)
            throws IOException, InterruptedException {
        System.out.println("INPUTFORMAT-GETSPLITS");
        List<InputSplit> splits = new ArrayList<InputSplit>(k);
        for (int i=0; i<k; i++) {
            splits.add(new JobJobberInputSplit(k));
        }
        return splits;
    }
    

}
