package com.renren.chargeMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChargeMapper 
	extends Mapper<LongWritable,Text,Text,DoubleWritable>{
	@Override
	public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
		String line = value.toString();
		if (line.indexOf('-') == -1){
			//read uid
			ListUserId.addUserId(line.trim());
		} else {
			String[] lines = line.split(",");
			if (lines.length == 4){
			String date = lines[0].replace("-", "");
			String userId = lines[1];
			double price = Double.parseDouble(lines[2]);
			String fromId = lines[3];
			//String outKey = date + "-" + fromId;
			String outKey = fromId;
			//if (ListUserId.isExist(userId)){
			if (Integer.parseInt(userId) < 890128587) {
				context.write(new Text(outKey), new DoubleWritable(price));
				}
			} else if (lines.length == 3){
				String date = lines[0].replace("-", "");
				String userId = lines[1];
				double price = Double.parseDouble(lines[2]);
				String outKey = "ios";
				if (Integer.parseInt(userId) < 890128587) {
					context.write(new Text(outKey), new DoubleWritable(price));
				}
			} else {
				//pass
			}
		}
	}
}
