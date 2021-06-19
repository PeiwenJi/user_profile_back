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

    //根据ID获取用户特征
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
        String ageGroup = hBaseClient.getValue("aft_basic_user", id, "user", "ageGroup");
        String constellation = hBaseClient.getValue("aft_basic_user", id, "user", "constellation");
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
        infoList.add(ageGroup);
        infoList.add(constellation);
        infoList.add(money);
        infoList.add(registerTime);
        infoList.add(lastLoginTime);

        return JSON.toJSONString(infoList);
    }

    //根据ID获取消费特征
    @RequestMapping("/searchConsumptionFeature")
    public String searchConsumptionFeature(String id){
        String consumptionCycle = hBaseClient.getValue("aft_basic_biz", id, "biz", "consumptionCycle");
        String avgOrderAmount = hBaseClient.getValue("aft_basic_biz", id, "biz", "avgOrderAmount");
        String paymentCode = hBaseClient.getValue("aft_basic_biz", id, "biz", "paymentCode");
        String maxOrderAmount = hBaseClient.getValue("aft_basic_biz", id, "biz", "maxOrderAmount");
        String spendingPower = hBaseClient.getValue("aft_basic_biz", id, "biz", "spendingPower");

        List<String> consumptionList = new ArrayList<>();
        consumptionList.add(consumptionCycle);
        consumptionList.add(avgOrderAmount);
        consumptionList.add(paymentCode);
        consumptionList.add(maxOrderAmount);
        consumptionList.add(spendingPower);

        return JSON.toJSONString(consumptionList);
    }

    //根据ID获取行为特征
    @RequestMapping("/searchBehaviorFeature")
    public String searchBehaviorFeature(String id){
        String lastLogin = hBaseClient.getValue("aft_basic_beh", id, "behavior", "lastLogin");
        String browsePage = hBaseClient.getValue("aft_basic_log", id, "log", "browsePage");
        String logSession = hBaseClient.getValue("aft_basic_beh", id, "behavior", "logSession");
        String goodsBought = hBaseClient.getValue("aft_basic_beh", id, "behavior", "goodsBought");
        String browseFrequency = hBaseClient.getValue("aft_basic_log", id, "log", "browseFrequency");
        String logFrequency = hBaseClient.getValue("aft_basic_log", id, "log", "logFrequency");
        String browseTime = hBaseClient.getValue("aft_basic_log", id, "log", "browseTime");

        List<String> behaviorList = new ArrayList<>();
        behaviorList.add(lastLogin);
        behaviorList.add(browsePage);
        behaviorList.add(logSession);
        behaviorList.add(goodsBought);
        behaviorList.add(browseFrequency);
        behaviorList.add(logFrequency);
        behaviorList.add(browseTime);

        return JSON.toJSONString(behaviorList);
    }

    //根据ID获取用户价值
    @RequestMapping("/searchUserValue")
    public String searchUserValue(String id){
        String userValue = hBaseClient.getValue("aft_basic_biz", id, "biz", "userValue");

        List<String> valueList = new ArrayList<>();
        valueList.add(userValue);

        return JSON.toJSONString(valueList);
    }
}
