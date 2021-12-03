package com.cpucode.mapreduce.reduceJoin;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author : cpucode
 * @date : 2021/12/3 15:15
 * @github : https://github.com/CPU-Code
 * @csdn : https://blog.csdn.net/qq_44226094
 */
@Data
public class TableBean implements Writable {
    /**
     * 订单id
     */
    private String id;

    /**
     * 商品id
     */
    private String pid;

    /**
     * 商品数量
     */
    private int amount;

    /**
     * 商品名称
     */
    private String pname;

    /**
     * 标记是什么表 order pd
     */
    private String flag;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        // id	pname	amount
        return  id + "\t" +  pname + "\t" + amount ;
    }

}
