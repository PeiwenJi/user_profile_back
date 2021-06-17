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
        tag.setFirst("电商");
        ResultScanner result = hBaseClient.selectTags(tag);
        ArrayList resultArray =new ArrayList();
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
            resultArray.add(columnMap);
        }
        result.close();
        if(dict =="true"){
            for(int i =0;i<resultArray.size();i++){
                Map<String, Object> dictMap = new HashMap<>();
                dictMap.put("value",resultArray.get(i));
                dictMap.put("label",resultArray.get(i));
            }
        }
        System.out.println(resultArray.size());
        return resultArray;
    }

}
