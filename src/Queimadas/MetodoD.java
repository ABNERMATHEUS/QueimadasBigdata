package Queimadas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MetodoD {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();

        Path input = new Path("in/Focos_2020-01-01_2020-10-14.csv");

        Path output = new Path("output/AAS.txt");

        Job j = new Job(c, "AAS");

        j.setJarByClass(MetodoD.class);//CLASSE PRINCIPAL
        j.setMapperClass(MapAAS.class);//REGISTRAR DA CLASSE MAP
        j.setReducerClass(ReduceAAS.class); //REGISTRO DA CLASSE REDUCE

        j.setMapOutputKeyClass(Text.class);//SAIDA CHAVE MAP
        j.setMapOutputValueClass(Auxiliar.class);// SAIDA VALOR MAP


        j.setCombinerClass(CombinerAAS.class);// Combiner

        j.setOutputKeyClass(Text.class); // SAIDA REDUCE CHAVE
        j.setOutputValueClass(FloatWritable.class); // SAIDA REDUCE VALOR

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);


        j.waitForCompletion(true);

    }

    public static class MapAAS extends Mapper<LongWritable, Text, Text, Auxiliar> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            if(line.startsWith("datahora")) return;

            String[] column = line.split(",");

            Float diassemchuva;
            if(column[6].isEmpty()){
                diassemchuva=Float.parseFloat("0");

            }else{
                diassemchuva = Float.parseFloat(column[6]);
            }

            String  estado = column[3];
            String bioma = column[5];
            if(!column[2].equals("Brasil")) return;

            con.write(new Text("Estado= "+estado+"; Bioma= "+bioma+"; médiadiaschuva="), new Auxiliar(1,diassemchuva));
        }
    }

    public static class ReduceAAS extends Reducer<Text, Auxiliar, Text, FloatWritable> {
        public void reduce(Text word, Iterable<Auxiliar> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;
            float dia=0;

            for(Auxiliar obj: values) {
                sum += obj.getN();
                dia+= obj.getVariavel();
            }

            float media= sum/dia;

            con.write(word, new FloatWritable(media));

        }
    }


    public static class CombinerAAS extends Reducer<Text, Auxiliar, Text, Auxiliar> {
        public void reduce(Text word, Iterable<Auxiliar> values, Context con)
                throws IOException, InterruptedException {

            int sum = 0;
            float dia=0;

            for(Auxiliar obj: values) {
                sum += obj.getN();
                dia+= obj.getVariavel();
            }

            con.write(word, new Auxiliar(sum,dia));

        }
    }

}
