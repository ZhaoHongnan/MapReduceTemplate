package com.renren.chargeMR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Charge 
{
    public static void main( String[] args ) throws Exception {
    	if (args.length != 2) {
    		System.err.println("Usage: Charge <input path> <output path> ");
    		System.exit(-1);
    	}
    	
    	Job job = new Job();
    	job.setJarByClass(Charge.class);
    	
    	FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(ChargeMapper.class);
        job.setReducerClass(ChargeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
