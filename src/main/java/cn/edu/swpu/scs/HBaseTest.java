package cn.edu.swpu.scs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTest {
    static Configuration conf = HBaseConfiguration.create();
    static Connection conn;

    public static void main(String[] args){
        //创建HBase配置
        //conf.set("hbase.rootdir", "hdfs://node1:9000/hbase");
        conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        //conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            //创建HBase连接
            conn = ConnectionFactory.createConnection(conf);
            //创建表
            //createTable();
            //写入单条数据（表名、列键、列族、列、值）
            //putSingleValue("Student", "stu01", "Score", "Bigdata", "80");
            //获取单条数据
            String value="1,2,4";
            String line=value.toString();
            String[] words = line.split(",");
            
            for(int i=0;i<words.length;i++) {
            	getSingleValue("Student", words[i], "Base", "Name");
            	System.out.print("(");
            	getSingleValue("Student", words[i], "Base", "Number");
            	System.out.print(")");
            	System.out.print(":");
            	getSingleValue("Student", words[i], "Score", "English");
            	System.out.print("  ");
            	getSingleValue("Student", words[i], "Score", "Java");
            	System.out.print("\n");
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    //创建表
    public static void createTable() throws IOException {
        //构造表名对象
        TableName tableName = TableName.valueOf("Student");
        //获取管理对象
        Admin admin = conn.getAdmin();
        //判断表是否存在
        if(!admin.tableExists(tableName)) {
            //构造表描述器器
            TableDescriptorBuilder tableDescipt = TableDescriptorBuilder.newBuilder(tableName);
            ////////////////////////////////////////////////////////////////////////////////////////////
            //构造列族描述器
            ColumnFamilyDescriptorBuilder columnFamilyDescript =  ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Score"));
            //创建列描述器
            ColumnFamilyDescriptor columnFamily = columnFamilyDescript.build();
            //添加列族
            tableDescipt.setColumnFamily(columnFamily);
            ////////////////////////////////////////////////////////////////////////////////////////////
            //获得表描述器
            TableDescriptor td = tableDescipt.build();
            //创建表
            admin.createTable(td);
        }else {
            System.out.println("表 " + tableName.getNameAsString() + " 已存在");
        }
    }

    //新增单条数据
    public static void putSingleValue(String tableStr, String rowKey, String columnFamily, String columnName, String cellValue) throws IOException{
        TableName tableName = TableName.valueOf(tableStr);
        Table table = conn.getTable(tableName);// Tabel负责跟记录相关的操作如增删改查等//
        Put put=new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(cellValue));
        table.put(put);
        table.close();
        System.out.println("add data Success!");
    }

    //删除单条数据
    public static void deleteSingleValue(String tableStr,String rowKey, String columnFamily, String columnName) throws IOException {
        TableName tableName = TableName.valueOf(tableStr);
        //创建表对象，实现表删除
        Table table = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));  //定义行
        //delete.addFamily(Bytes.toBytes(columnFamily));    //定义需要删除的列族
        //delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));   //定义需要删除的列
        table.delete(delete);
        table.close();
    }

    //删除表
    public static  void deleteTable(String tableStr) throws IOException{
        TableName tableName = TableName.valueOf(tableStr);
        //创建管理对象并禁用表，删除表前先禁用掉
        Admin admin = conn.getAdmin();
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    //获取单条数据
    public static void getSingleValue(String tableStr, String rowKey, String columnFamily, String columnName) throws IOException {
        TableName tableName = TableName.valueOf(tableStr);
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.addFamily(Bytes.toBytes(columnFamily));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName));
        //执行数据读取并返回结果对象
        Result set = table.get(get);
        //获取一行数据集中的所有单元格(cell)对象
        Cell[] cells  = set.rawCells();
        //遍历所有单元格对象
        for(Cell cell : cells) {
            //System.out.println(Bytes.toString(cell.getRowArray()));
            //System.out.println(Bytes.toString(cell.getFamilyArray()));
            //System.out.println(Bytes.toString(cell.getQualifierArray()));
            //System.out.println(Bytes.toString(cell.getValueArray()));
            byte[] cellValue = cell.getValueArray();
            String row = Bytes.toString(cellValue, cell.getRowOffset(), cell.getRowLength());
            String family = Bytes.toString(cellValue, cell.getFamilyOffset(), cell.getFamilyLength());
            String column = Bytes.toString(cellValue, cell.getQualifierOffset(), cell.getQualifierLength());
            Long timestamp = cell.getTimestamp();
            String value = Bytes.toString(cellValue, cell.getValueOffset(), cell.getValueLength());
            System.out.print(String.format("%s", value));
        }
        table.close();
    }
}