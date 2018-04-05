package mapreduce;

//dataguru 05c倒排索引

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class test_1 extends Configured implements Tool {
	enum Counter//自增操作，
	{
		LINESKIP//出错的行，记录出错的行
	}
	public static class Map extends Mapper<LongWritable,Text,Text,Text>//
	{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException
		{
			String line = value.toString();
			try
			{
				String[] lineSplit = line.split("");//135，10086
				String anum = lineSplit[0];
				String bnum = lineSplit[1];
				context.write(new Text(bnum),new Text(anum));//输出
			}
			catch(java.lang.ArrayIndexOutOfBoundsException e)//e是自己定义的异常对象的名字，异常生成后是一个异常对象，系统传递给我们，传递到e中，catch越界的异常
			{
				context.getCounter(Counter.LINESKIP).increment(1);//出错令计数器+1
			}
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text>values,Context context)throws IOException,InterruptedException
		{//Text key, Iterable<Text>values必须与Reducer<Text,Text,Text,Text>前两个Text一致。
			String valueString;
			String out="";
			for(Text value:values)
			{
				valueString = value.toString();
				out+=valueString + "|";				
			}
			context.write(key,new Text(out));
		}
	}
	@Override
	public int run(String[] args)throws Exception
	{
		Configuration conf = getConf();
		
		Job job = new Job(conf,"test_1");//任务名
		job.setJarByClass(test_1.class);//指定class
		FileInputFormat.addInputPath(job,new Path(args[0]));//输入路径
		FileOutputFormat.setOutputPath(job,new Path(args[1]));//输出路径
		
		job.setMapperClass(Map.class);			//调用上面Map类作为Map任务代码
		job.setReducerClass(Reduce.class);		//调用上面Reduce类作为Reduce任务代码
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);		//指定输出的KEY的格式，对应Reducer中后两个
		job.setOutputValueClass(Text.class);	//指定输出的VALUE的格式
		
		job.waitForCompletion(true);
		return job.isSuccessful()?0:1;
	}
	public static void main(String[] args)throws Exception
	{
		int re = ToolRunner.run(new Configuration(),new test_1(),args);
		System.exit(re);
	}
}		
		
		Configuration conf = getConf();
		Job job = new Job(conf,"data deduplication");
		job.setJarByClass(Dedup.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputKeyPath(job,new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)? 0:1);

		Configuration conf = getConf();
		Job job = new Job(conf, "test_1");				//任务名，jar包后面的输入、输出路径对应下面的addInputPath，setOutputPath
		job.setJarByClass(test_1.class);				//指定class
		FileInputFormat.addInputPath(job,new Path(args[0]));	//输入路径
		FileOutputFormat.setOutputPath(job,new Path(args[1]));	//输出路径
		job.setMapperClass(Map.class);					//调用上面的Map类作为Map任务代码
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutKeyClass(NullWritable.class);			//指定输出的KEY的格式
		job.setOutputValueClass(Text.class)				//指定输出的VALUE的格式
		
		job.waitForCompletion(true);
		return job.isSuccessful()?0:1;
		
		Job job = new Job(conf,"word count");//新生成一个作业叫word count
		//配置作业的各个类
		job.setJarByClass(wordcount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombineClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);


















