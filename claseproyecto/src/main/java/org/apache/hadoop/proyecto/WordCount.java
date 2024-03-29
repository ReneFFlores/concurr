package org.apache.hadoop.proyecto;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
    public static class MiMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class MiMapperDos
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            String primeraPalabra = "";
            String segundaPalabra = "";
            if (itr.hasMoreTokens()) {
                primeraPalabra = itr.nextToken();
            }
            while (itr.hasMoreTokens()) {
                segundaPalabra = itr.nextToken();
                word.set(primeraPalabra + ' ' + segundaPalabra);
                context.write(word, one);
                primeraPalabra = segundaPalabra;
            }
        }
    }


    public static class MiReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // args: <archivo entrada> <archivo salida> <una o dos palabras>
        if (args.length < 3) {
            System.exit(2);
        }

        boolean modoUna = args[2].equals("una");

        Job job = Job.getInstance(conf, "conteo de palabras");

        job.setJarByClass(WordCount.class);

        if (modoUna) {
            job.setMapperClass(MiMapper.class);
        } else {
            job.setMapperClass(MiMapperDos.class);
        }

        job.setCombinerClass(MiReducer.class);
        job.setReducerClass(MiReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // archivo de entrada
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // archivo de salida
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
