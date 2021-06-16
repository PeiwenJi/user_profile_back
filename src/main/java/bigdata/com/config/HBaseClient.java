package bigdata.com.config;

import bigdata.com.bean.Tag;
import com.sun.corba.se.impl.logging.IORSystemException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

@Component
@DependsOn("hbaseConfig")
public class HBaseClient {

    @Resource
    private HbaseConfig config;

    private static Admin admin = null;
    private static Connection connection = null;

    @PostConstruct
    private void init() {
        if (connection != null) {
            return;
        }
        try {
            connection = ConnectionFactory.createConnection(config.configuration());
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createTable(String tableName, String... columnFamilies) throws IOException {
        TableName name = TableName.valueOf(tableName);
        boolean isExists = this.tableExists(tableName);
        if (isExists) {
            throw new TableExistsException(tableName + "is exists!");
        }
        TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(name);
        List<ColumnFamilyDescriptor> columnFamilyList = new ArrayList<>();
        for (String columnFamily : columnFamilies) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder
                    .newBuilder(columnFamily.getBytes()).build();
            columnFamilyList.add(columnFamilyDescriptor);
        }
        descriptorBuilder.setColumnFamilies(columnFamilyList);
        TableDescriptor tableDescriptor = descriptorBuilder.build();
        admin.createTable(tableDescriptor);
    }

    public void insertOrUpdate(String tableName, String rowKey, String columnFamily, String column, String value) throws IOException {
        this.insertOrUpdate(tableName, rowKey, columnFamily, new String[]{column}, new String[]{value});
    }

    //修改用户信息
    public void insertOrUpdate(String tableName, String rowKey, String columnFamily, String[] columns, String[] values)
            throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < columns.length; i++) {
            put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
            table.put(put);
        }
    }

    //删除指定行
    public void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        table.delete(delete);
    }

    public void deleteColumnFamily(String tableName, String rowKey, String columnFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
    }

    public void deleteColumn(String tableName, String rowKey, String columnFamily, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(rowKey.getBytes());
        delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        table.delete(delete);
    }
    public String getValue(String tableName, String rowkey, String family, String column) {
        Table table;
        String value = "";
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family) || StringUtils.isBlank(rowkey) || StringUtils.isBlank(column)) {
            return null;
        }
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get g = new Get(rowkey.getBytes());
            g.addColumn(family.getBytes(), column.getBytes());
            Result result = table.get(g);
            List<Cell> ceList = result.listCells();
            if (ceList != null && ceList.size() > 0) {
                for (Cell cell : ceList) {
                    value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }

    public void deleteTable(String tableName) throws IOException {
        boolean isExists = this.tableExists(tableName);
        if (!isExists) {
            return;
        }
        TableName name = TableName.valueOf(tableName);
        admin.disableTable(name);
        admin.deleteTable(name);
    }

    //根据指定规则选取用户信息
    public ResultScanner selectUsers(String company,String email,String identity) {

        Table table;
        String tableName="user";
        ResultScanner rs =null;
        List<Filter> filters =new ArrayList<>();
        filters.add(new SingleColumnValueFilter(Bytes.toBytes("basic"),    //family
                        Bytes.toBytes("identity"),                         //列
                        CompareFilter.CompareOp.EQUAL,identity.getBytes()));     //值
        //以公司为检索条件
        if(company != null && company.length()!=0){
            filters.add(new SingleColumnValueFilter(Bytes.toBytes("basic"),    //family
                    Bytes.toBytes("company"),                         //列
                    CompareFilter.CompareOp.EQUAL,company.getBytes()));     //值
        }
        //以邮箱作为检索条件  rowkey
        if(email != null && email.length()!=0 ){
            filters.add(new RowFilter(CompareFilter.CompareOp.EQUAL ,
                    new BinaryComparator(email.getBytes())));     //值
        }

        FilterList filterList =new FilterList(filters);

        if (StringUtils.isBlank(tableName) ) {

            return null;
        }
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            rs = table.getScanner(scan);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return rs;
    }

    /**
     * 按照指定规则选取标签
     */
    public ResultScanner selectTags(Tag tag){
        Table table;
        String tableName="tag";
        ResultScanner rs =null;
        List<Filter> filters =new ArrayList<>();

        if(tag.getFirst() != null && tag.getFirst().length()!=0){
            filters.add(new SingleColumnValueFilter(Bytes.toBytes("basic"),    //family
                    Bytes.toBytes("first"),                         //列
                    CompareFilter.CompareOp.EQUAL,tag.getFirst().getBytes()));     //值
        }
        if(tag.getSecond() != null && tag.getSecond().length()!=0){
            filters.add(new SingleColumnValueFilter(Bytes.toBytes("basic"),    //family
                    Bytes.toBytes("second"),                         //列
                    CompareFilter.CompareOp.EQUAL,tag.getSecond().getBytes()));     //值
        }
        if(tag.getThird() != null && tag.getThird().length()!=0){
            filters.add(new SingleColumnValueFilter(Bytes.toBytes("basic"),    //family
                    Bytes.toBytes("third"),                         //列
                    CompareFilter.CompareOp.EQUAL,tag.getThird().getBytes()));     //值
        }
        if(tag.getForth() != null && tag.getForth().length()!=0){
            filters.add(new SingleColumnValueFilter(Bytes.toBytes("basic"),    //family
                    Bytes.toBytes("forth"),                         //列
                    CompareFilter.CompareOp.EQUAL,tag.getForth().getBytes()));     //值
        }
        if(tag.getStatus() != null && tag.getStatus().length()!= 0){
            filters.add(new SingleColumnValueFilter(Bytes.toBytes("basic"),    //family
                    Bytes.toBytes("status"),                         //列
                    CompareFilter.CompareOp.EQUAL,tag.getStatus().getBytes()));     //值
        }
        FilterList filterList =new FilterList(filters);

        if (StringUtils.isBlank(tableName) ) {
            return null;
        }
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filterList);
            rs = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rs;
    }
    /**
     * 用于统计每天注册的用户数目
     * @return ArrayList<ResultScanner>前一天...前10天  截止到当天的用户列表
     * @throws IOException
     */
    public ArrayList<ResultScanner> countUserChanged() throws IOException {
        Table table = connection.getTable(TableName.valueOf("user"));
        ResultScanner rs =null;
        ArrayList<ResultScanner> resultLIst= new ArrayList<>();
        List<TimestampsFilter> timestampsFilterList =new ArrayList<>();
        long current =System.currentTimeMillis();
        for (int i=0;i<11;i++)
        {
            List<Long> list = new ArrayList<>();
            list.add(current-5184000*i);
            timestampsFilterList.add(new TimestampsFilter(list));
        }
        try{
            Scan scan =new Scan();
            for(int i =0;i<timestampsFilterList.size();i++)
            {
                scan.setFilter(timestampsFilterList.get(i));
                rs = table.getScanner(scan);
                resultLIst.add(rs);
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return resultLIst;
    }

    public String selectOneRow(String tableName, String rowKey, String colFamily) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(), "name".getBytes());
        get.addColumn(colFamily.getBytes(), "age".getBytes());
        Result result = table.get(get);
        table.close();
        if (result == null) {
            return "";
        }
        return new String(result.getValue(colFamily.getBytes(), "name".getBytes())) + "-" + new String(result.getValue(colFamily.getBytes(), "age".getBytes()));
    }

    public boolean tableExists(String tableName) throws IOException {
        TableName[] tableNames = admin.listTableNames();
        if (tableNames != null && tableNames.length > 0) {
            for (TableName name : tableNames) {
                if (tableName.equals(name.getNameAsString())) {
                    return true;
                }
            }
        }
        return false;
    }

}
