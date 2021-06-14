package bigdata.com.controller;

import bigdata.com.bean.User;
import bigdata.com.config.HBaseClient;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;

@RestController
public class LoginController {
    @Resource
    private HBaseClient hBaseClient;

    //登录
    @RequestMapping("/login")
    public String login(String email, String password){

        //根据email在user数据库中搜索用户
        String result = hBaseClient.getValue("user", email, "basic", "password");

        //检查用户数据库中是否有该用户
        //如果没有，提示用户注册
        if(result == "")
            return "This account doesn't exist";
        else {
            //如果有，检查密码是否正确
            if(!result.equals(password))
                return "Password error";
            else
                return "success";
        }
    }

    //注册
    @RequestMapping("/register")
    public String register(@RequestBody User user){

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
                hBaseClient.insertOrUpdate("user", user.getEmail(), "basic", "identity", "user");

                return "success";
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return "fail";
    }
}
