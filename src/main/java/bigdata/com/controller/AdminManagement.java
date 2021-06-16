package bigdata.com.controller;

import bigdata.com.bean.User;
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
public class AdminManagement {
    @Resource
    private HBaseClient hBaseClient;

    @RequestMapping("/getAdminList")
    public ArrayList getAdminList(){
        ResultScanner result = hBaseClient.getAllAdmin("user");

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
                //resultMap.put(rowKey,columnMap);
                columnMap.put("email",rowKey);
            }
            resultArray.add(columnMap);
        }
        result.close();

//        System.out.println(resultArray);
        return resultArray;
    }

    @RequestMapping("/addAdmin")
    public String addAdmin(@RequestBody User user) {
        try {
            System.out.println(user);
            hBaseClient.insertOrUpdate("user",user.getEmail(),"basic","name", user.getName());
            hBaseClient.insertOrUpdate("user",user.getEmail(),"basic","password", user.getPassword());
            hBaseClient.insertOrUpdate("user",user.getEmail(),"basic","email", user.getEmail());
            hBaseClient.insertOrUpdate("user",user.getEmail(),"basic","company", user.getCompany());
            hBaseClient.insertOrUpdate("user",user.getEmail(),"basic","identity", user.getIdentity());
//            hBaseClient.createTable("userPermission","basic");
//            hBaseClient.insertOrUpdate("userPermission","user","basic","id", "1");
//            hBaseClient.insertOrUpdate("userPermission","user","basic","identity", "user");
//            hBaseClient.insertOrUpdate("userPermission","user","basic","description", "user description");
//            hBaseClient.insertOrUpdate("userPermission","admin","basic","id", "2");
//            hBaseClient.insertOrUpdate("userPermission","admin","basic","identity", "admin");
//            hBaseClient.insertOrUpdate("userPermission","admin","basic","description", "admin description");
//            hBaseClient.insertOrUpdate("userPermission","super-admin","basic","id", "3");
//            hBaseClient.insertOrUpdate("userPermission","super-admin","basic","identity", "super-admin");
//            hBaseClient.insertOrUpdate("userPermission","super-admin","basic","description", "super-admin description");
            System.out.println("ok");
            return "ok";
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");
            return "error";
        }
    }

    @RequestMapping("/deleteAdmin")
    public String deleteAdmin(String email) {
        try {
            hBaseClient.deleteRow("user", email);
            System.out.println("success");
            return "ok";
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("error");
            return "error";
        }
    }
}
