package bigdata.com.controller;

import bigdata.com.config.HBaseClient;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.IOException;

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
}
