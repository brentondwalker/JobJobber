package jobjobber;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class JobJobberInputSplit extends InputSplit implements Writable {

    public int k;
    
    /**
     * Empty constructor
     */
    public JobJobberInputSplit() {
        super();
        System.out.println("INPUTSPLIT");
    }
    
    /**
     * Constructor
     * 
     * @param k
     */
    public JobJobberInputSplit(int k) {
        super();
        this.k = k;
        System.out.println("INPUTSPLIT");
    }
    
    @Override
    public long getLength() throws IOException, InterruptedException {
        System.out.println("INPUTSPLIT-GETLENGTH");
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        System.out.println("INPUTSPLIT-GETLOCATIONS");
        //String[] locations = { "localhost" };
        String[] locations = { };
        return locations;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
        System.out.println("INPUTSPLIT-READFIELDS");

        // TODO Auto-generated method stub
        
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        System.out.println("INPUTSPLIT-WRITE");
        // TODO Auto-generated method stub
        
    }

}
