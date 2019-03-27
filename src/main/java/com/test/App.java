package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.DoubleColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author lvxw
 */
public class App {

    private static Configuration conf = HBaseConfiguration.create();
    private static Connection connect = null;
    private static Admin admin = null;

    static {
        try {
            conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
            connect = ConnectionFactory.createConnection(conf);
            admin = connect.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testInsert(){
        try {
            Table table = connect.getTable(TableName.valueOf("test"));
            Put put = new Put(Bytes.toBytes("004"));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("lisi"));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(22));
            put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("score"),Bytes.toBytes(99.6));
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testScan(){
        try{
            Table table = connect.getTable(TableName.valueOf("test"));
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes("001"));
            scan.withStartRow(Bytes.toBytes("003"));
            scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("cf1"))));
            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                byte[] row = result.getRow();
                byte[] name = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
                byte[] age = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
                byte[] score = result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("score"));
                System.out.println(Bytes.toString(row)+","+Bytes.toString(name)+","+Bytes.toInt(age)+","+Bytes.toDouble(score));
                System.out.println(result.toString());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void testGet(){
        try{
            Table table = connect.getTable(TableName.valueOf("test"));
            Get get = new Get(Bytes.toBytes("003"));
            get.addFamily(Bytes.toBytes("cf1"));

            Result result = table.get(get);
            System.out.println(result.toString());

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * hbase shell 添加：
     *      alter 'test', METHOD => 'table_att','coprocessor'=>'|org.apache.hadoop.hbase.coprocessor.AggregateImplementation||'
     */
    public static void testProcessor(){
        try {
            AggregationClient aggregationClient = new AggregationClient(conf);
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("score"));
            long rowCount = aggregationClient.rowCount(TableName.valueOf("test"), new LongColumnInterpreter(), scan);
            double avgAge = aggregationClient.min(TableName.valueOf("test"), new DoubleColumnInterpreter(), scan);
            System.out.println("row count is " + rowCount);
            System.out.println("avg age is " + avgAge);
        }catch (Throwable e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception{
//        testInsert();
//        testScan();
//        testGet();
        testProcessor();
    }
}
