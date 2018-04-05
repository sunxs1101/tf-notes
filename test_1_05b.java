//dataguru 05b

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
/*
Writable是hadoop的核心，还有很多其他的Writable类，比如WritableComparable,ArrayWritable,Two-DArrayWritable及AbstractMapWritable，它们直接继承自Writable类。
还有一些类，如BooleanWritable,ByteWritable类，它们不是直接继承于Writable类，而是继承自WritableCamparable类。
*/
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class test_1 extends Configured implements Tool {//约定俗成
	enum Counter//自增操作，
	{
		LINESKIP//出错的行，记录出错的行
	}
	
	//map类+map方法
	public static class Map extends Mapper<longwritable,Text,NullWritable,Text>//longwritable、text等在io.*中
	{//longwrite:输入key的格式，Text:输入value的格式，可以想象成string格式；NullWritable,Text：输出格式。一般前两个是不变的，主要修改后两个输出格式。
	//longwritable，intwritable：分别对应java中的long和int格式。NullWritable相当于空值。Text:这是hadoop中对string类型的重写，但是又与其有一些不同。	
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException
		{//LongWritable key, Text value和Mapper中的前两个对应。
		//输入文本的行号即偏移量，内容记录在key中；内容被记录在value中，这个例子中：key相当于行号，value是内容。
			String line = value.toString()//读取源数据
			try
			{
				//数据处理
				String []lineSplit = line.split("");
				String month = lineSplit[0];
				String time = lineSplit[1];
				String mac = lineSplit[6];
				Text out = new Text(month + "" + time + "" + mac);//输出不能用string，转化成hadoop能读取的格式
				
				//context.write(key,value),context上下文机制。标准输出格式：key \t value，如果key是空值，就不打印制表符。
				context.write(NullWritable.get(),out);//输出key value。NullWritable.get()表示key是空值的实例。
				
			}
			catch(java.lang.ArrayIndexOutOfBoundsException e)//越界错误
			{
				context.getCounter(Counter.LINESKIP).increment(1);//出错令计数器+1
				return;
			}
		}
	}
	/*
	context是mapper的一个内部类，简单的说顶级接口是为了在map或是reduce任务中跟踪task的状态，很自然的MapContext就是记录了map执行的上下文，
	在mapper类中，这个context可以存储一些job conf的信息，比如习题一中的运行时参数等，我们可以在map函数中处理这个信息，这也是hadoop中参数
	传递中一个很经典的例子，同时context作为了map和reduce执行中各个函数的一个桥梁，这个设计和java web中的session对象、application对象很相似。
	*/
	@Override
	public int run(String[] args) throws Exception//run函数，作用：让程序正确运行，正确提交mapreduce任务。需要修改的部分并不是很多。
	{
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
	}
	public static void main(String[] args) throws Exception
	{
		//运行任务
		int res = ToolRunner.run(new Configuration(),new Test_1(),args);//调用上面的run函数，run函数的作用是做一些基本的配置。
		System.exit(res);
	}
}



























