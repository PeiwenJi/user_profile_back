package bigdata.com.controller;

import bigdata.com.bean.User;
import bigdata.com.config.HBaseClient;
import com.alibaba.fastjson.JSON;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

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

    //获取用户登录信息
    @RequestMapping("/getUserInfo")
    public String getUserInfo(String email){
        String name, company, identity;

        name = hBaseClient.getValue("user", email, "basic", "name");
        company = hBaseClient.getValue("user", email, "basic", "company");
        identity = hBaseClient.getValue("user", email, "basic", "identity");

        Map<String, Object> res = new HashMap<>();
        res.put("flag", "success");
        res.put("name", name);
        res.put("company", company);
        res.put("identity", identity);

        return JSON.toJSONString(res);
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

    //获取邮箱验证码
    @RequestMapping("/getCode")
    public String getCode(String email) {

        //随机生成验证码
        String code = String.valueOf(new Random().nextInt(899999) + 100000);
        //生成验证信息
        String message = "您的验证码为："+code;

        HtmlEmail htmlEmail = new HtmlEmail();
        try {
            // 设置SMTP服务器
            htmlEmail.setHostName("smtp.qq.com");
            //端口号
            htmlEmail.setSmtpPort(465);
            //开启SSL加密
            htmlEmail.setSSLOnConnect(true);
            htmlEmail.setCharset("utf-8");
            // 发件人信息
            htmlEmail.setFrom("peiwenji0408@qq.com", "那年夏天，那抹蓝");
            //发件人授权码
            htmlEmail.setAuthentication("peiwenji0408@qq.com", "huicctgslbtxjfbc");
            // 设置主题
            htmlEmail.setSubject("邮箱验证");
            // 设置邮件内容
            htmlEmail.setHtmlMsg(message);
            //收件人邮箱
            htmlEmail.addTo(email);
            // 发送邮件
            htmlEmail.send();
            return code;
        } catch (EmailException e) {
            return "fail";
        }
    }

    //重置密码
    @RequestMapping("/reset")
    public String reset(String email, String password){
        //根据email在user数据库中搜索用户
        String result = hBaseClient.getValue("user", email, "basic", "password");

        //检查用户数据库中是否有该用户
        //如果没有，提示用户注册
        if(result == "")
            return "This account doesn't exist";
        else {
            try {
                hBaseClient.insertOrUpdate("user", email, "basic", "password", password);
                return "success";
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return "fail";
    }
}
