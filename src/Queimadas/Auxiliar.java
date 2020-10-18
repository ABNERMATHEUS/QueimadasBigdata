package Queimadas;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Auxiliar implements Writable {

    private Integer n;

    private Float variavel;

    public Integer getN() {
        return n;
    }

    public void setN(Integer n) {
        this.n = n;
    }

    public Float getVariavel() {
        return variavel;
    }

    public void setVariavel(Float variavel) {
        this.variavel = variavel;
    }

    public Auxiliar(int n, float variavel) {
        this.n = n;
        this.variavel = variavel;
    }

    public Auxiliar() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(n.toString());
        dataOutput.writeUTF(variavel.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.n = Integer.parseInt(dataInput.readUTF());
        this.variavel = Float.parseFloat(dataInput.readUTF());
    }



}
