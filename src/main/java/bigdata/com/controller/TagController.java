package bigdata.com.controller;


import bigdata.com.bean.Tag;
import bigdata.com.config.HBaseClient;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
        }

        //规范查找到的四级标签格式
        if(dict.contentEquals("true")){

            //去除掉四级标签list当中的重复部分
            for(int i =0;i<resultArray.size()-1;i++){
                for(int j=resultArray.size()-1;j>i;j--){
                    if(resultArray.get(i).equals(resultArray.get(j)))
                        resultArray.remove(j);
                }
            }
        }
        result.close();
        //System.out.println(resultArray.size());
        return resultArray;
    }


    //管理员编辑标签信息
    @RequestMapping("/editTagInfo")
    public String editTagInfo(@RequestBody Tag tag){
        String[] columns={"first","second","third","forth","fifth","status"};
        System.out.println("editTagInfo"+tag.getId());
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

            Tag brotherTag = new Tag(Double.parseDouble(rowKey),columnMap.get("first").toString(),columnMap.get("second").toString(),columnMap.get("third").toString()
                    ,columnMap.get("forth").toString(),columnMap.get("fifth").toString(),columnMap.get("status").toString());
            tagArray.add(brotherTag);
        }
        tag.setStatus(status);
        return tagArray;
    }

}
