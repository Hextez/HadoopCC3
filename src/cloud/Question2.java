package cloud;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import cloud.asd.E_EMapper;
import cloud.asd.E_EReduce;

public class Question2 {

	//Mapper class 
	public static class E_EMapper extends MapReduceBase implements 
	Mapper<LongWritable ,/*Input key Type */ 
	Text,                /*Input value Type*/ 
	Text,                /*Output key Type*/ 
	Text>        /*Output value Type*/ 
	{ 

		//Map function 
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, Text> output,   
				Reporter reporter) throws IOException 
		{ 
			String line = value.toString(); 
			String bytes = null;
			String time = null;
			StringTokenizer s = new StringTokenizer(line,","); 
			String ip_source = s.nextToken(); 
			String ip_dest = s.nextToken();
			int i = 0;
			int c = s.countTokens();
			while(s.hasMoreTokens() && i < 5)
			{
				if (i == 4){
					bytes=s.nextToken();

				}
				s.nextToken();
				i++;
			} 

			//System.out.println(avgprice);
			// System.out.println(time);

			output.collect(new Text(ip_source), new Text(ip_dest+":"+bytes)); 
		} 
	} 


	//Reducer class 
	public static class E_EReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	// Reducer< Text, IntWritable, Text, IntWritable > 
	{  

		//Reduce function 
		public void reduce( Text key, Iterator <Text> values, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{ 

			HashMap<String, Long> s = new HashMap<>();
			//int val=Integer.MIN_VALUE; 
			while (values.hasNext()){ 
				String va = values.next().toString();
				String[] o = va.split(":");
				
				if (s.containsKey(o[0])){
					s.put(o[0], s.get(o[0]) + Long.parseLong(o[1]));
				}else{
					
					s.put(o[0], Long.parseLong(o[1]));
				}


			} 
			for (String keys : s.keySet()) {
				//System.out.println(s.get(keys).get("source")+":"+s.get(keys).get("dest"));
				output.collect(key,new Text(keys+":"+s.get(keys)));
			}
			//System.out.println(string);

		} 
	} 
	public static class reduceRR extends MapReduceBase implements Reducer<Text,Text,Text,Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			long sourceSize = 0;
			String s = null;
			//long destSize = 0;
			while (values.hasNext()){
				
				String[] sizes = values.next().toString().split(":");
				s = sizes[0];
				sourceSize += Long.parseLong(sizes[1]);
				
			}
			
			output.collect(new Text(key+":"+s), new Text(String.valueOf(sourceSize)));

		}

		public static class GroupComparator extends Text.Comparator {
			 
	        @Override
	        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	            int cmp = super.compare(b1, s1, l1, b2, s2, l2);
	            System.err.println("Group cmp: " + new String(b1, s1, l2) + " ?= "
	                    + new String(b2, s2, l2) + " = " + cmp);
	            return cmp;
	        }
	 
	    }
	 
	    public static class SortComparator extends Text.Comparator {
	 
	        @Override
	        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	            int cmp = super.compare(b1, s1, l1, b2, s2, l2);
	            System.err.println("Sort cmp: " + new String(b1, s1, l2) + " ?= "
	                    + new String(b2, s2, l2) + " = " + cmp);
	            return cmp;
	        }
	 
	    }
		//Main function 
		public static void main(String args[])throws Exception 
		{ 
			JobConf conf = new JobConf(asd.class); 

			conf.setJobName("max_eletricityunits"); 
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(E_EMapper.class); 
			conf.setCombinerClass(E_EReduce.class); 
			conf.setReducerClass(reduceRR.class); 
			//conf.setCombinerKeyGroupingComparator(GroupComparator.class);
			conf.setInputFormat(TextInputFormat.class); 
			conf.setOutputFormat(TextOutputFormat.class); 

			FileInputFormat.setInputPaths(conf, new Path("input/data.csv"));
			try {
				File file = new File("output");
				file.delete();
			}catch(Exception e){

			}

			FileOutputFormat.setOutputPath(conf, new Path("output")); 

			JobClient.runJob(conf); 


		} 
	}
}
