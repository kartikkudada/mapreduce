package mapreduce;

import java.io.*;
import java.math.BigInteger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
 
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
        int count = 0;
        for(IntWritable val : values) {
            count = count + val.get();
        }
        
        result.set(count);
        try {
			//context.write(key, new IntWritable(count));
        	context.write(key, result);
		} catch (InterruptedException e) {
		
			e.printStackTrace();
		}     
	
    }
}