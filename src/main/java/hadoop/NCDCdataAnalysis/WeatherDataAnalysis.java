package hadoop.NCDCdataAnalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


/**
 * A BigData job that generates the maximum temperature recorded on a yearly basis
 *
 */
public class WeatherDataAnalysis 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
    {
    	if(args.length!=2)
    	{
    		System.out.println("Insufficient number of arguments passed");
    		System.exit(1);
    	}    	
    	String input = args[0];
    	String output = args[1];
    	Configuration jobConfig = new Configuration();    	
    	jobConfig.set("mapreduce.task.profile","true");
    	jobConfig.set("mapreduce.task.profile.maps","0-1" );
        Job weatherAnalysis = new Job(jobConfig,"weatherDataAnalysis");
        weatherAnalysis.setJarByClass(WeatherDataAnalysis.class);
        weatherAnalysis.setJobName("NCDCweatherDataAnalysis");
        weatherAnalysis.setMapperClass(MaxTemperatureMapper.class);
        weatherAnalysis.setReducerClass(MaxTemperatureReducer.class);
        weatherAnalysis.setCombinerClass(MaxTemperatureReducer.class);
        weatherAnalysis.setPartitionerClass(HashPartitioner.class);
        weatherAnalysis.setNumReduceTasks(2);
        weatherAnalysis.setMapOutputKeyClass(Text.class);
        weatherAnalysis.setMapOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(weatherAnalysis, new Path(input));
        FileOutputFormat.setOutputPath(weatherAnalysis, new Path(output));
        
        System.exit(weatherAnalysis.waitForCompletion(true) ? 0 : 1);
        
    }
}
