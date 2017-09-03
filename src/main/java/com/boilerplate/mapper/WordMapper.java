package com.boilerplate.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by henAl on 9/2/17.
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable inkey, Text invalue, Context context) throws IOException, InterruptedException {

        String input[] =invalue.toString().split(",",4);
        context.write(new Text(input[0]),new IntWritable(Integer.valueOf(input[3])));
    }
}
