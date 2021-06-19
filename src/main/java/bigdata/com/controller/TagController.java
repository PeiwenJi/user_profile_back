package bigdata.com.controller;


import bigdata.com.bean.Tag;
import bigdata.com.config.HBaseClient;
import com.alibaba.fastjson.JSON;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.poi.xssf.usermodel.XSSFSheet;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

//注意需要加上这个注解
@RestController
public class TagController {
    @Resource
    private HBaseClient hBaseClient;

    /**
     * 从tags.xlsx文件当中读取标签信息之后写入hbase
     */
    @RequestMapping("/initTags")
    public String  initTags(){
        //System.out.println("进入函数initTags" );
        String[] values=new String[6];
        String[] columns ={"first","second","third","forth","fifth","status"};
        try {
            FileInputStream  fileInputStream = new FileInputStream("C:\\Users\\Administrator\\Desktop\\bigdata_userprofile\\src\\main\\resources\\tags.xlsx");
            XSSFWorkbook sheets = new XSSFWorkbook(fileInputStream);
            XSSFSheet sheet = sheets.getSheet("Sheet1"); //获取sheet
            int rows =sheet.getPhysicalNumberOfRows();  //获取总的行数
            System.out.println(rows);
            for(int i =0;i<rows;i++)
            {
                XSSFRow row1 = sheet.getRow(i);
                String id =row1.getCell(0).toString();
                for(int j=1;j<7;j++){
                    values[j-1]=row1.getCell(j).toString();
                }
                hBaseClient.insertOrUpdate("tag",id,"basic",columns,values);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return"success";
    }

    /**
     * 管理员获取标签数据
     * @param tag
     * @return
     */
    @RequestMapping("/showTags")
    public ArrayList showTags(@RequestBody Tag tag, @RequestParam(value = "dict",required = false,defaultValue = "false") String dict) {
        if(tag.getThird().contentEquals("全部")){
            tag.setThird("");
        }
        if(tag.getForth().contentEquals("全部")){
            tag.setForth("");
        }
        tag.setFirst("电商");
        //System.out.println(dict);
//        System.out.println(dict+tag.getFirst());
//        System.out.println(dict+tag.getStatus());
        ResultScanner result = hBaseClient.selectTags(tag);
        ArrayList resultArray =new ArrayList();

        if(dict.contentEquals("true")){
            Map<String, Object> dictMap = new HashMap<>();
            dictMap.put("value","全部");
            dictMap.put("label","全部");
            resultArray.add(dictMap);
        }
        for(Result res : result) {
            //System.out.println(1);
            Map<String, Object> columnMap = new HashMap<>();
            String rowKey = null;
            for (Cell cell : res.listCells()) {
                if (rowKey == null) {
                    rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                }
                columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            if (rowKey != null) {
                columnMap.put("id",rowKey);
            }
            if(dict.contentEquals("false")){
                resultArray.add(columnMap);
            }
            if(dict.contentEquals("true")){
                Map<String, Object> dictMap = new HashMap<>();
                dictMap.put("value",columnMap.get("forth"));
                dictMap.put("label",columnMap.get("forth"));
                resultArray.add(dictMap);
            }
            if(dict.contentEquals("tagWorld")){
                Map<String, Object> dictMap = new HashMap<>();
                dictMap.put("value",Double.parseDouble(columnMap.get("id").toString()));
                dictMap.put("name",columnMap.get("fifth"));
                resultArray.add(dictMap);
            }
        }

        //规范查找到的四级标签格式
        if(dict.contentEquals("true") || dict.contentEquals("tagWorld")){
            //去除掉四级标签list当中的重复部分
            for(int i =0;i<resultArray.size()-1;i++){
                for(int j=resultArray.size()-1;j>i;j--){
                    if(resultArray.get(i).equals(resultArray.get(j)))
                        resultArray.remove(j);
                }
            }
        }


        result.close();
//        System.out.println(dict+resultArray.size());
        return resultArray;
    }


    //管理员编辑标签信息
    @RequestMapping("/editTagInfo")
    public String editTagInfo(@RequestBody Tag tag){
        String[] columns={"first","second","third","forth","fifth","status"};
        //System.out.println("editTagInfo"+tag.getId());
        ArrayList<Tag> brotherTag =getSameForthIdList(tag);

        try {
            for(int i =0;i<brotherTag.size();i++)
            {
               // System.out.println(tag.getStatus());
                String[] values ={brotherTag.get(i).getFirst(),brotherTag.get(i).getSecond(),
                        brotherTag.get(i).getThird(),brotherTag.get(i).getForth(),brotherTag.get(i).getFifth(),tag.getStatus()};
                //System.out.println(values[5]);
                hBaseClient.insertOrUpdate("tag",String.valueOf(brotherTag.get(i).getId()),"basic",columns,values);
            }
            return "success";
        } catch (IOException e) {
            e.printStackTrace();
            return "error";
        }
    }

    /**
     * 返回与该五级标签同四级标签的五级标签的列表
     * @param tag
     * @return
     */
    public ArrayList<Tag> getSameForthIdList(Tag tag){
        String status = tag.getStatus();
        tag.setFifth("");
        tag.setStatus("");
        ResultScanner result = hBaseClient.selectTags(tag);
        ArrayList<Tag> tagArray =new ArrayList();
        for(Result res : result) {
            Map<String, Object> columnMap = new HashMap<>();
            String rowKey = null;
            for (Cell cell : res.listCells()) {
                if (rowKey == null) {
                    rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                }
                columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            }
//            System.out.println(JSON.toJSONString(columnMap));
//            System.out.println(rowKey);
            Tag brotherTag = new Tag(Double.parseDouble(rowKey),columnMap.get("first").toString(),columnMap.get("second").toString(),columnMap.get("third").toString()
                    ,columnMap.get("forth").toString(),columnMap.get("fifth").toString(),columnMap.get("status").toString());
            tagArray.add(brotherTag);
        }
        tag.setStatus(status);
        return tagArray;
    }

    /**
     * 删除单个五级标签操作
     */
    @RequestMapping("/deleteSingleFifthTag")
    public String deleteSingleFifthTag  (@RequestParam("tagId") String tagId){
        System.out.println(tagId);
        try {
            hBaseClient.deleteRow("tag",tagId);
            return "success";
        } catch (IOException e) {
            e.printStackTrace();
            return "fail";
        }

    }

    /**
     * 管理员创建新的四级标签
     */
    @RequestMapping("/createNewComposedTag")
    public String createNewComposedTag  (@RequestParam("initId") String initId,@RequestParam("first")String first,
                                         @RequestParam("second") String second,@RequestParam("third")String third,
                                         @RequestParam("forth") String forth,@RequestParam("fifth")String fifth){

//        System.out.println(initId);
//        System.out.println(fifth);
        String[] fifthList = fifth.split(",");

        //没有相同名字、相同上级的四级标签
        Tag tag =new Tag(Double.parseDouble(initId),first,second,third,forth,"","");
        ResultScanner sameNameForth = hBaseClient.selectTags(tag);
        //ArrayList<String> sameNameForthFIfthList=new ArrayList<>();
        for(Result res:sameNameForth){
            return "name complicate";
        }

        //相同上级下相同五级标签的四级标签

        for (int i =0;i<fifthList.length;i++){
            String insertFifth= fifthList[i];
            double id = Double.parseDouble(initId) +(double) i +1;
            String[] columns={"first","second","third","forth","fifth","status"};
            String[] values={first,second,third,forth,insertFifth,"unpassed"};
            try {
                hBaseClient.insertOrUpdate("tag",String.valueOf(id),"basic",columns,values);
            } catch (IOException e) {
                e.printStackTrace();
                return "something happened in creating composed tags";

            }
        }
        return "success";

    }

    /**
     * 点击词云直接跳转到对应的标签区域里面
     */
    @RequestMapping("/getTagWorldPath")
    public String[] getTagWorldPath(@RequestParam("id") String id){

        String first = hBaseClient.getValue("tag",String.valueOf(Double.parseDouble(id)),"basic","first");
        String second = hBaseClient.getValue("tag",String.valueOf(Double.parseDouble(id)),"basic","second");
        String third = hBaseClient.getValue("tag",String.valueOf(Double.parseDouble(id)),"basic","third");
        String forth = hBaseClient.getValue("tag",String.valueOf(Double.parseDouble(id)),"basic","forth");
        String fifth = hBaseClient.getValue("tag",String.valueOf(Double.parseDouble(id)),"basic","fifth");
        String status = hBaseClient.getValue("tag",String.valueOf(Double.parseDouble(id)),"basic","status");
        String[] result ={first,second,third,forth,fifth,status};
        return result;
    }

    /**
     * 获取编辑框当前标签的历史编辑状态
     */
    @RequestMapping("getSelectedTagHistory")
    public ArrayList getSelectedTagHistory(@RequestParam("id") String id)
    {
        //System.out.println("func :getSelectedTagHistory RequestParam id: "+id);
        ArrayList<Map<String,String>> resultList= hBaseClient.getSelectedTagHistory(String.valueOf(Double.parseDouble(id)));
        //System.out.println("func :getSelectedTagHistory resultList size: "+resultList.size());
        //将timestamp规范化
        for (int i = 0;i<resultList.size();i++){
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy MM dd HH mm ss");
            Date date =new Date(Long.parseLong(resultList.get(i).get("timestamp")));
            String res=simpleDateFormat.format(date);
            //System.out.println(res);
            int currentDay=Integer.valueOf(res.split(" ")[2]);
            int currentMonth=Integer.valueOf(res.split(" ")[1]);
            int currentYear=Integer.valueOf(res.split(" ")[0]);
            resultList.get(i).put("timestamp",currentYear +"-"+currentMonth +"-"+currentDay+" "+Integer.valueOf(res.split(" ")[3])+
                    ":"+Integer.valueOf(res.split(" ")[4])+":"+Integer.valueOf(res.split(" ")[5]));
        }

        return resultList;
    }

    /**
     * 获取标签库当中的五级标签的信息
     */
    @RequestMapping("/getComposedTagsInfo")
    public  ArrayList getComposedTagsInfo(){
        Tag tag = new Tag(0,"","","组合标签","","","");
        ResultScanner result = hBaseClient.selectTags(tag);
        ArrayList<Map> finalList =new ArrayList<>();
        ArrayList<Map> composedTanInfoList =new ArrayList();
        for(Result res : result) {
            Map<String, Object> columnMap = new HashMap<>();
            String rowKey = null;
            for (Cell cell : res.listCells()) {
                if (rowKey == null) {
                    rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                }
                columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            }
            composedTanInfoList.add(columnMap);
        }
        System.out.println(JSON.toJSONString(composedTanInfoList));
        Set<String> composedForthNameSet = new HashSet<>();
        ArrayList<String> composedForthNameList =new ArrayList<>();
        for (int i =0;i<composedTanInfoList.size();i++) {
            composedForthNameSet.add(composedTanInfoList.get(i).get("forth").toString());
        }
        List<String> simplifiedForthTagName = new ArrayList<>(composedForthNameSet);
        for(int i =0;i<simplifiedForthTagName.size();i++){
            String status =null;
            Map<String,Object> oneComposedTagMap =new HashMap<>();
            ArrayList<String> oneComposedTagFifthList =new ArrayList<>();
            for(int j =0;j<composedTanInfoList.size();j++){
                if(composedTanInfoList.get(j).get("forth").toString().contentEquals(simplifiedForthTagName.get(i))){
                    status=composedTanInfoList.get(j).get("status").toString();
//                    System.out.println("四级标签"+simplifiedForthTagName.get(i));
//                    System.out.println("加入的:"+composedTanInfoList.get(j).get("fifth").toString());
                    oneComposedTagFifthList.add(composedTanInfoList.get(j).get("fifth").toString());
                }
            }
            oneComposedTagMap.put("forth",simplifiedForthTagName.get(i));
            oneComposedTagMap.put("fifth",oneComposedTagFifthList);
            oneComposedTagMap.put("status",status);
            finalList.add(oneComposedTagMap);
        }

//        System.out.println(composedForthNameList);
//        System.out.println(JSON.toJSONString(composedForthNameSet));
//        System.out.println(JSON.toJSONString(finalList));
        return finalList;
    }

    /**
     * 获取满足组合标签要求的用户列表信息
     */
    @RequestMapping("/getComposedTagsUserList")
    public ArrayList  getComposedTagsUserList(@RequestParam(value = "composedTagName",required= false) String composedTagName){
        Map<String,String> tagAndColumnName = new HashMap<>();
        tagAndColumnName.put("消费能力爆表的中年男士","80sManMaxOrder");
        tagAndColumnName.put("经常访问的狮子女士","oftenLeoFemale");
        ResultScanner result= hBaseClient.getComposedUserList(tagAndColumnName.get(composedTagName));
        ArrayList<Map> resultArray =new ArrayList<>();
        for(Result res : result) {
            Map<String, Object> columnMap = new HashMap<>();
            String rowKey = null;
            for (Cell cell : res.listCells()) {
                if (rowKey == null) {
                    rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                }
                columnMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()), Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));

            }
            resultArray.add(columnMap);
        }
        //System.out.println(JSON.toJSONString(resultArray));
        return  resultArray;
    }

}
