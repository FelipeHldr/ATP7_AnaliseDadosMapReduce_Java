/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pucpr.atp;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author felipe.alberto
 */
public class Informacao4 {
    
    public static class MapperInformacao extends Mapper<Object, Text, Text, IntWritable> {
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            try {
                String[] campos = valor.toString().split(";");
                if (campos.length == 10) {
                    
                    Text chaveCampo = new Text(campos[3]);
                    IntWritable valorMap = new IntWritable(1);
                    context.write(chaveCampo, valorMap);
                }
            } catch (IOException | InterruptedException | NumberFormatException err) {
                System.out.println(err);
            }
        }
        
    }
    
    public static class ReducerInformacao extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            int i = 0;
            
            for (IntWritable intvalores : valores) {
                i += intvalores.get();
            }
            context.write(chave, new IntWritable(i));
        }
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/felipe.alberto/Desktop/implementacaoLocalMR/informacao4";
        
        if (args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
            
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao4");
        
        job.setJarByClass(Informacao4.class);
        job.setMapperClass(MapperInformacao.class);
        job.setReducerClass(ReducerInformacao.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        job.waitForCompletion(true);
        
        
    }
}
    

