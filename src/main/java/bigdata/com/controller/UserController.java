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
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class UserController {
    @Resource
    private HBaseClient hBaseClient;

    //用户修改账户信息
    @RequestMapping("/edit")
    public String edit(String email, String name, String company){
        try {
            hBaseClient.insertOrUpdate("user", email, "basic", "name", name);
            hBaseClient.insertOrUpdate("user", email, "basic", "company", company);

            return "success";
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "fail";
    }

    //用户修改个人密码
    @RequestMapping("/resetPassword")
    public String resetPassword(String email, String new_password){
        try {
            hBaseClient.insertOrUpdate("user", email, "basic", "password", new_password);

            return "success";
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "fail";
    }

    //管理员获取用户信息
    @RequestMapping("/showUsers")
    public ArrayList showUsers(@RequestParam(value = "company",required = false)String company,
                               @RequestParam(value = "email",required = false)String email,
                               @RequestParam(value = "identity",required = false,defaultValue = "user")String identity){
        ResultScanner result = hBaseClient.selectUsers(company,email,identity);
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

    //管理员编辑用户信息
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

    //管理员删除用户
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

    //管理员添加用户
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

    /**
     * 统计每天新增的用户数目
     * @return ArrayList<Integer> 前9天.....前一天 新增用户数目
     */
    @RequestMapping("/countUserChanged")
 public ArrayList<Object> countUserChanged(){
        ArrayList<Object> result =new ArrayList<>();

        /**不能使用depracted 方法getDay等，获取出来的日期、月份数据等是不正确的*/
        //获取当前时间
        long current =System.currentTimeMillis();
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy MM dd HH mm ss");
        Date date =new Date(current);
        String res=simpleDateFormat.format(date);
        int currentDay=Integer.valueOf(res.split(" ")[2]);
        int currentMonth=Integer.valueOf(res.split(" ")[1]);

        //创建时间字符串
        String[] dateList =new String[10];
        for(int i =9;i>=0;i--)
        {
            long before= current-86400000L*i;
            Date dateBefore =new Date(before);
            String dateBeforeRes=simpleDateFormat.format(dateBefore);
            int beforeDay=Integer.valueOf(dateBeforeRes.split(" ")[2]);
            int beforeMonth=Integer.valueOf(dateBeforeRes.split(" ")[1]);
            dateList[9-i]=String.valueOf(beforeMonth)+"月"+String.valueOf(beforeDay)+"日";
        }

        //统计前十天每天的新增人数
        int[] count=new int[10];
        ResultScanner resultScanner = hBaseClient.selectUsers(null,null,"user");
        for (Result rs:resultScanner){
            Date rsDate =new Date(rs.listCells().get(0).getTimestamp());
            String dateBeforeRes=simpleDateFormat.format(rsDate);
            int rsDay=Integer.valueOf(dateBeforeRes.split(" ")[2]);
            int rsMonth=Integer.valueOf(dateBeforeRes.split(" ")[1]);
            if(rsMonth==currentMonth && (currentDay-rsDay)<10 && (currentDay-rsDay)>=0){
                count[9-(currentDay-rsDay)]++;
            }
        }
        result.add(dateList);
        result.add(count);
     return result;
 }

}
