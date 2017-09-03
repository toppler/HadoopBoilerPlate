package com.boilerplate.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by henAl on 9/2/17.
 */
public class WordReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count=0;
        for(IntWritable val : values) {
            count += val.get();
        }
        context.write(key,new IntWritable(count));
    }
}
