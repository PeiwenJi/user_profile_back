package bigdata.com.controller;

import bigdata.com.config.HBaseClient;
import com.alibaba.fastjson.JSON;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

@RestController
public class DataController {
    @Resource
    private HBaseClient hBaseClient;

    //获取男女分布
    @RequestMapping("/getGender")
    public String getGender() {
        String maleNum = hBaseClient.getValue("final", "gender_male", "cf", "val");
        String femaleNum = hBaseClient.getValue("final", "gender_female", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(maleNum);
        result.add(femaleNum);

        return JSON.toJSONString(result);
    }

    //获取政治面貌分布
    @RequestMapping("/getPoliticalFace")
    public String getPoliticalFace() {
        String num_1 = hBaseClient.getValue("final", "群众", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "党员", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "无党派人士", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);

        return JSON.toJSONString(result);
    }

    //获取婚姻状况
    @RequestMapping("/getMarriage")
    public String getMarriage() {
        String num_1 = hBaseClient.getValue("final", "离异", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "已婚", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "未婚", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);

        return JSON.toJSONString(result);
    }

    //获取星座发布
    @RequestMapping("/getConstellation")
    public String getConstellation() {
        String num_1 = hBaseClient.getValue("final", "双子座", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "双鱼座", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "处女座", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "天秤座", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "天蝎座", "cf", "val");
        String num_6 = hBaseClient.getValue("final", "射手座", "cf", "val");
        String num_7 = hBaseClient.getValue("final", "巨蟹座", "cf", "val");
        String num_8 = hBaseClient.getValue("final", "摩羯座", "cf", "val");
        String num_9 = hBaseClient.getValue("final", "水瓶座", "cf", "val");
        String num_10 = hBaseClient.getValue("final", "狮子座", "cf", "val");
        String num_11 = hBaseClient.getValue("final", "白羊座", "cf", "val");
        String num_12 = hBaseClient.getValue("final", "金牛座", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);
        result.add(num_6);
        result.add(num_7);
        result.add(num_8);
        result.add(num_9);
        result.add(num_10);
        result.add(num_11);
        result.add(num_12);

        return JSON.toJSONString(result);
    }

    //获取工作分布
    @RequestMapping("/getJob")
    public String getJob() {
        String num_1 = hBaseClient.getValue("final", "学生", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "公务员", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "军人", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "警察", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "教师", "cf", "val");
        String num_6 = hBaseClient.getValue("final", "白领", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);
        result.add(num_6);

        return JSON.toJSONString(result);
    }

    //获取年龄分布
    @RequestMapping("/getAge")
    public String getAge() {
        String num_1 = hBaseClient.getValue("final", "50后", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "60后", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "70后", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "80后", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "90后", "cf", "val");
        String num_6 = hBaseClient.getValue("final", "00后", "cf", "val");
        String num_7 = hBaseClient.getValue("final", "10后", "cf", "val");
        String num_8 = hBaseClient.getValue("final", "20后", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);
        result.add(num_6);
        result.add(num_7);
        result.add(num_8);

        return JSON.toJSONString(result);
    }

    //获取消费周期分布
    @RequestMapping("/getCycle")
    public String getCycle() {
        String num_1 = hBaseClient.getValue("final", "近7日", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "近2周", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "近1月", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "近2月", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "近3月", "cf", "val");
        String num_6 = hBaseClient.getValue("final", "近4月", "cf", "val");
        String num_7 = hBaseClient.getValue("final", "近5月", "cf", "val");
        String num_8 = hBaseClient.getValue("final", "近6月", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);
        result.add(num_6);
        result.add(num_7);
        result.add(num_8);

        return JSON.toJSONString(result);
    }

    //获取浏览时段分布
    @RequestMapping("/getLogSession")
    public String getLogSession() {
        String num_1 = hBaseClient.getValue("final", "0-7点", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "8-12点", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "13-17点", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "18-21点", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "22-24点", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);

        return JSON.toJSONString(result);
    }

    //获取支付方式分布
    @RequestMapping("/getPaymentCode")
    public String getPaymentCode() {
        String num_1 = hBaseClient.getValue("final", "alipay", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "wxpay", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "cod", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);

        return JSON.toJSONString(result);
    }

    //获取客单价分布
    @RequestMapping("/getAvgAmount")
    public String getAvgAmount(){
        String num_1 = hBaseClient.getValue("final", "单价：1-999", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "单价：1000-2999", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "单价：3000-4999", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "单价：5000-9999", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "单价：10000-", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);

        return JSON.toJSONString(result);
    }

    //获取客单价分布
    @RequestMapping("/getMaxAmount")
    public String getMaxAmount(){
        String num_1 = hBaseClient.getValue("final", "最高：1-999", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "最高：1000-2999", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "最高：3000-4999", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "最高：5000-9999", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "最高：10000-", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);

        return JSON.toJSONString(result);
    }
}
