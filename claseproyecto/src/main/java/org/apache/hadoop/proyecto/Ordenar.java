package org.apache.hadoop.proyecto;

import java.nio.ByteBuffer;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class Ordenar {
    public static class MiComparador extends WritableComparator {
        public MiComparador() {
            super(IntWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
            Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
            return v1.compareTo(v2) * (-1);
        }
    }

    public static class MiMapperDos
            extends Mapper<LongWritable, Text, IntWritable, Text> {

        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String primeraPalabra = itr.nextToken();
            String segundaPalabra = itr.nextToken();

            int vecesInt = Integer.parseInt(itr.nextToken());
            if (vecesInt >= 5000) {
                IntWritable veces = new IntWritable(vecesInt);
                context.write(veces, new Text(primeraPalabra + ' ' + segundaPalabra));
            }
        }
    }

    public static class MiMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String primeraPalabra = itr.nextToken();
            int vecesInt = Integer.parseInt(itr.nextToken());

            if (vecesInt >= 5000) {
                IntWritable veces = new IntWritable(vecesInt);
                context.write(veces, new Text(primeraPalabra));

            }
        }
    }

    public static class MiReducer
            extends Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> list, Context context) throws java.io.IOException, InterruptedException {
            for (Text value : list) {
                context.write(value, key);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(true);
        if (args.length < 3) {
            System.out.println("Argumentos: <archivo entrada> <carpeta salida> <una o dos>");
            System.exit(2);
        }

        boolean modoUna = args[2].equals("una");

        Job job = Job.getInstance(conf, "ordenar output");
        job.setJarByClass(Ordenar.class);

        if (modoUna) {
            job.setMapperClass(MiMapper.class);
        } else {
            job.setMapperClass(MiMapperDos.class);
        }

        job.setReducerClass(MiReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(IntWritable.class);

        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(MiComparador.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
