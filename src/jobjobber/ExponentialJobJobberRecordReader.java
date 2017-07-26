package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Random;


/**
 * The record reader is returned by the InputFormat for each InputSplit.
 * In normal operation this would be reading and parsing files or something,
 * or getting data from a database.  But in this case it will just return
 * samples from a random inter-time distribution.
 * import java.util.Random;

 * @author brenton
 *
 */
public class ExponentialJobJobberRecordReader extends RecordReader<LongWritable, DoubleWritable> {
    
    public double rate = 1.0;
    int task_id_counter = 0;
    double current_value = 0.0;
    public static Random random = new Random();
    private boolean read_once = false;

    /**
     * Constructor
     * 
     * @param rate
     */
    public ExponentialJobJobberRecordReader(double rate) {
        this.rate = rate;
        System.out.println("RECORDREADER");
    }
    
    @Override
    public void close() throws IOException {
        System.out.println("RECORDREADER-CLOSE");

        // nothing to close
    }

    @Override
    public float getProgress() throws IOException {
        System.out.println("RECORDREADER-GETPROGRESS");

        // noting to do
        // would be nice if it could return the fraction of the total time
        // this task has been running, but given the architecture, that
        // can't really be known here; it is in the Mapper.
        // Also there probably is no reason for it.
        if (read_once) return 1.0f;
        return 0.0f;
    }

    /*
    @Override
    public long getPos() throws IOException {
        // nothing to do here
        return 0;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable(0);
    }

    @Override
    public DoubleWritable createValue() {
        return new DoubleWritable(0.0);
    }

    @Override
    public boolean next(LongWritable key, DoubleWritable value) throws IOException {
        key.set(task_id_counter++);
        value.set(-Math.log(random.nextDouble())/rate);
        return true;
    }
    */

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        System.out.println("RECORDREADER-GETCURRENTKEY");
        return new LongWritable(task_id_counter);
    }

    @Override
    public DoubleWritable getCurrentValue() throws IOException, InterruptedException {
        System.out.println("RECORDREADER-GETCURRENTVALUE");

        return new DoubleWritable(current_value) ;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        System.out.println("RECORDREADER-INIT");

        read_once = false;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        System.out.println("RECORDREADER-NEXTKEYVALUE");

        task_id_counter++;
        current_value = -Math.log(random.nextDouble())/rate;

        // this RecordReader is set up to only return a single record and return false after that
        if (read_once) return false;
        read_once = true;
        return true;
    }

}
