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
        String num_1 = hBaseClient.getValue("final", "politicalFace_masses", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "politicalFace_partyMember", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "politicalFace_nonparty", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);

        return JSON.toJSONString(result);
    }

    //获取婚姻状况
    @RequestMapping("/getMarriage")
    public String getMarriage() {
        String num_1 = hBaseClient.getValue("final", "marriage_divorced", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "marriage_discoverture", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "marriage_married", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);

        return JSON.toJSONString(result);
    }

    //获取国籍发布
    @RequestMapping("/getNationality")
    public String getNationality() {
        String num_1 = hBaseClient.getValue("final", "nationality_china", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "nationality_hongkong", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "nationality_macao", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "nationality_taiwan", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "nationality_others", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);

        return JSON.toJSONString(result);
    }

    //获取工作分布
    @RequestMapping("/getJob")
    public String getJob() {
        String num_1 = hBaseClient.getValue("final", "job_student", "cf", "val");
        String num_2 = hBaseClient.getValue("final", "job_civilServant", "cf", "val");
        String num_3 = hBaseClient.getValue("final", "job_soldier", "cf", "val");
        String num_4 = hBaseClient.getValue("final", "job_police", "cf", "val");
        String num_5 = hBaseClient.getValue("final", "job_teacher", "cf", "val");
        String num_6 = hBaseClient.getValue("final", "job_witheCollar", "cf", "val");

        List<String> result = new ArrayList<>();
        result.add(num_1);
        result.add(num_2);
        result.add(num_3);
        result.add(num_4);
        result.add(num_5);
        result.add(num_6);

        return JSON.toJSONString(result);
    }
}
