package bigdata.com.controller;

import bigdata.com.bean.UserPermission;
import bigdata.com.config.HBaseClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class RolePermission {
    @Resource
    private HBaseClient hBaseClient;

    @RequestMapping("/getRoleList")
    public ArrayList getRoleList(){
        ResultScanner result = hBaseClient.getAllRole("userPermission");

        ArrayList resultArray =new ArrayList();
        Map<String, Object> resultMap = new HashMap<>();
        for(Result res : result) {
            Map<String, Object> columnMap = new HashMap<>();
            String rowKey = null;
            for (Cell cell : res.listCells()) {
                if (rowKey == null) {
                    rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                }
                columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }

            if (rowKey != null) {
//                resultMap.put(rowKey,columnMap);
                columnMap.put("identity",rowKey);
            }
            resultArray.add(columnMap);
        }
        result.close();

//        System.out.println(resultArray);
        return resultArray;
    }

    @RequestMapping("/editRole")
    public String editRole(@RequestBody UserPermission userPermission) {
        try {
            hBaseClient.insertOrUpdate("userPermission",userPermission.getIdentity(),"basic","identity", userPermission.getIdentity());
            hBaseClient.insertOrUpdate("userPermission",userPermission.getIdentity(),"basic","description", userPermission.getDescription());
            hBaseClient.insertOrUpdate("userPermission",userPermission.getIdentity(),"basic","permission", userPermission.getPermission());
            System.out.println("ok");
            return "ok";
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");
            return "error";
        }
    }

}
