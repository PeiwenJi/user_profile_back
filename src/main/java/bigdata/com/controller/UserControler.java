package bigdata.com.controller;

import bigdata.com.bean.User;
import bigdata.com.config.HBaseClient;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@RestController
public class UserControler {
    @Resource
    private HBaseClient hBaseClient;


    @RequestMapping("/showUsers")
    public ArrayList  showUsers(@RequestParam(value = "company",required = false)String company, @RequestParam(value = "email",required = false)String email){

        ResultScanner result = hBaseClient.selectUsers(company,email);
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

        return resultArray;
    }

    @RequestMapping("/editUserInfo")
    public String editUserInfo(@RequestBody User user){
        String[] columns={"name","company","password"};
        String[] values ={user.getName(),user.getCompany(),user.getPassword()};
        try {
            hBaseClient.insertOrUpdate("user",user.getEmail(),"basic",columns,values);
            return "sucess";
        } catch (IOException e) {
            e.printStackTrace();
            return "error";
        }
    }

    @RequestMapping("/deleteUser")
    public String deleteUserInfo(@RequestParam(value = "email") String email){

        try {
            hBaseClient.deleteRow("user",email);
            return "success";
        } catch (IOException e) {
            e.printStackTrace();
            return "error";
        }
    }
    @RequestMapping("/addUser")
    public String addUser(@RequestBody User user){
        //根据email在user数据库中搜索用户
        String result = hBaseClient.getValue("user", user.getEmail(), "basic", "password");

        //检查用户数据库中是否有该用户
        //如果有，提示用户不能重复注册
        if(result != "")
            return "This account has already existed";
        else {
            //如果没有，允许注册
            try {
                hBaseClient.insertOrUpdate("user", user.getEmail(), "basic", "name", user.getName());
                hBaseClient.insertOrUpdate("user", user.getEmail(), "basic", "company", user.getCompany());
                hBaseClient.insertOrUpdate("user", user.getEmail(), "basic", "password", user.getPassword());
                hBaseClient.insertOrUpdate("user", user.getEmail(), "basic", "identity", user.getIdentity());
                hBaseClient.insertOrUpdate("user", user.getEmail(), "basic", "company", user.getCompany());
                return "success";
            } catch (IOException e) {
                e.printStackTrace();
                return "fail";

            }
        }


    }
}
