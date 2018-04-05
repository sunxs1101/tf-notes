package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class wordcount {
	//继承Mapper接口，设置map的输入类型为<object,text>
	//输出类型为<Text,IntWritable>
	public static class TokenizerMapper	extends Mapper<Object,Text,Text,IntWritable>{
		//one表示单词出现一次
		private final static IntWritable one = new IntWritable(1);
		//word用于存储切下的单词
		private Text word = new Text();
		public void map(Object key,Text value,Context context)  {
			StringTokenizer itr = new StringTokenizer(value.toString());//对输入的行切词
			while(itr.hasMoreTokens()){
				word.set(itr.nextToken());
				context.write(word,one);
			}
		}
	}
	//
	//继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	//输出类型为<Text, IntWritable>
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		//result记录单词的频数
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable>values,Context context)
		throws IOException, InterruptedException{
			int sum = 0;
			//对获取的<key,value-list>计算value的和
			for (IntWritable val: values){
				sum += val.get();
			}
			//将频数设置到result中
		result.set(sum);
		//收集结果
		context.write(key,result);
		}
	}
	
	//第3部分：怎样运行map-reduce过程，基本上都是固定模式
	public static void main (String[] args) throws Exception{
		Configuration conf = new Configuration();
		//检查运行命令
		if(args.length != 2){
			System.err.println("Usage:wordcount<in><out>");
			System.exit(2);
		}
		//配置作业名
			Job job = new Job(conf,"word count");//新生成一个作业叫word count
		//配置作业的各个类
			job.setJarByClass(wordcount.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job,new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			System.exit(job.waitForCompletion(true)?0:1);
	}
}


public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{     
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		System.out.println("key = " + key.toString());//添加查看key值
        System.out.println("value = " + value.toString());//添加查看value值
        StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one); 
		}     
	} 
}













