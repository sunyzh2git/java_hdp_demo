package com.sogou.demo;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;


class TempMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	private Pattern splitter = Pattern.compile("\t");
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		String line = new String(value.getBytes(), 0, value.getLength(), "GBK");
		String[] fields = splitter.split(line);
		if (fields.length < 15) {
			System.out.println("lenghth is less 15");
			return ;
		}
		String posVal = fields[12];
		int pos = Integer.parseInt(posVal);
		if (pos == 0) {
			String pid = fields[2];
			String sumVal = fields[14];
			int consume = Integer.parseInt(sumVal);
			output.collect(new Text(pid), new IntWritable(consume));
		}
	}
}

class TempReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		int sum = 0;
		
		while(values.hasNext()) {
			sum += values.next().get();
		}

		output.collect(new Text(key), new IntWritable(sum));
	}
	
}

public class StatChargeLog {
	public static void main(String[] args) throws Exception {

		// T ODO Auto-generated method stub
		String INPUT_CD_CHARGE_LOG = "/data/bidding/cd_charge_log/201701/20170116/cd_charge_log";

		String OUT_CHARGE_DIR = "/adta/sunyongzhen/test_javahdp/20170116";
		
		
		JobConf conf = new JobConf(StatChargeLog.class);
		//maserti
//		conf.setJobName("galaxyota_adta_sunyongzhen");
//		conf.set("hadoop.job.ugi", "adta,k7b3d$zw^v");
//		conf.set("mapred.job.tracker", "hdfs://maserati.hadoop.jt.sogou:18312");
//		conf.set("fs.default.name", "hdfs://maserati.hadoop.platform.sogou:18310");
		
		
		conf.setJobName("galaxyota_adta_sunyongzhen");
		conf.set("hadoop.job.ugi", "adta,ex6trvh#2l");
		conf.set("mapred.job.tracker", "hdfs://benz.hadoop.jt.sogou:18312");
		conf.set("fs.default.name", "hdfs://benz.hadoop.platform.sogou:18310");
		
		FileInputFormat.addInputPath(conf, new Path(INPUT_CD_CHARGE_LOG));
        FileOutputFormat.setOutputPath(conf, new Path(OUT_CHARGE_DIR));

        conf.setMapperClass(TempMapper.class);
        conf.setReducerClass(TempReducer.class);
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
        

	}

}
