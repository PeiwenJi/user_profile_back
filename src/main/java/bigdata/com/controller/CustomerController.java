package bigdata.com.controller;

import bigdata.com.config.HBaseClient;
import com.alibaba.fastjson.JSON;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@RestController
public class CustomerController {
    @Resource
    private HBaseClient hBaseClient;

    @RequestMapping("/searchCustomerInfo")
    public String searchCustomerInfo(String id) {
        String userName = hBaseClient.getValue("aft_basic_user", id, "user", "username");
        String gender = hBaseClient.getValue("aft_basic_user", id, "user", "gender");
        String birthday = hBaseClient.getValue("aft_basic_user", id, "user", "birthday");
        String email = hBaseClient.getValue("aft_basic_user", id, "user", "email");
        String mobile = hBaseClient.getValue("aft_basic_user", id, "user", "mobile");
        String job = hBaseClient.getValue("aft_basic_user", id, "user", "job");
        String nationality = hBaseClient.getValue("aft_basic_user", id, "user", "nationality");
        String marriage = hBaseClient.getValue("aft_basic_user", id, "user", "marriage");
        String politicalFace = hBaseClient.getValue("aft_basic_user", id, "user", "politicalFace");
        String source = hBaseClient.getValue("aft_basic_user", id, "user", "source");
        String qq = hBaseClient.getValue("aft_basic_user", id, "user", "qq");
        String money = hBaseClient.getValue("aft_basic_user", id, "user", "money");
        String registerTime = hBaseClient.getValue("aft_basic_user", id, "user", "registerTime");
        String lastLoginTime = hBaseClient.getValue("aft_basic_user", id, "user", "lastLoginTime");

        List<String> infoList = new ArrayList<>();
        infoList.add(userName);
        infoList.add(gender);
        infoList.add(birthday);
        infoList.add(email);
        infoList.add(mobile);
        infoList.add(job);
        infoList.add(nationality);
        infoList.add(marriage);
        infoList.add(politicalFace);
        infoList.add(source);
        infoList.add(qq);
        infoList.add(money);
        infoList.add(registerTime);
        infoList.add(lastLoginTime);

        return JSON.toJSONString(infoList);
    }
}
