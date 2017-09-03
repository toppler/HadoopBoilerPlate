package com.boilerplate;

import com.boilerplate.mapper.WordMapper;
import com.boilerplate.reducer.WordReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



/**
 * Created by henAl on 9/2/17.
 */
public class Application {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Application <input path> <output path>");
            System.exit(-1);
        }

        // Create the job specification object
        Job job = new Job();
        job.setJarByClass(Application.class);
        job.setJobName("Hadoop Boiler Plate Demo");

        // Setup input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the Mapper and Reducer classes
        job.setMapperClass(WordMapper.class);

        job.setReducerClass(WordReducer.class);

        // Specify the type of output keys and values
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Wait for the job to finish before terminating
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
