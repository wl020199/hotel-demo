package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelDocumentTest {
    @Autowired
    private IHotelService hotelService;

    private RestHighLevelClient restHighLevelClient;

    @BeforeEach
    void setUp(){
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://119.23.69.42:9200")));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.restHighLevelClient.close();
    }

    @Test
    void testAddDocument() throws IOException {
        Hotel hotel = hotelService.getById(61083L);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 1.准备request对象
        IndexRequest indexRequest = new IndexRequest("hotel").id(hotel.getId().toString());
        // 2.准备json文档
        indexRequest.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        // 3.发送请求
        restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);

    }


    @Test
    void testGetDocumentById() throws IOException {
        // 1.准被getrequest对象
        GetRequest getRequest = new GetRequest("hotel","61083");
        // 2.发送请求
        GetResponse response = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        String source = response.getSourceAsString();
        HotelDoc hotelDoc = JSON.parseObject(source, HotelDoc.class);
        System.out.println(hotelDoc);

    }

    @Test
    void testUpdateDocument() throws IOException {
        // 1.准备updaterequest对象
        UpdateRequest request = new UpdateRequest("hotel", "61083");
        // 2.准备更新内容
        request.doc(
                "price", 952
        );
        // 3.发送请求
        restHighLevelClient.update(request,RequestOptions.DEFAULT);
    }

    @Test
    void testDeleteDocument() throws IOException {
        // 1.准备deleterequest对象
        DeleteRequest request = new DeleteRequest("hotel", "61083");
        // 2.发送请求
        restHighLevelClient.delete(request,RequestOptions.DEFAULT);
    }

    @Test
    void addDocument() throws IOException {
        // 1.查出所有数据
        List<Hotel> hotels = hotelService.list();
        // 2.便利封装
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 3.准被indexRequest对象,内容, 发送请求
            IndexRequest indexRequest = new IndexRequest("hotel").id(hotel.getId().toString());
            indexRequest.source(JSON.toJSONString(hotelDoc),XContentType.JSON);
            restHighLevelClient.index(indexRequest,RequestOptions.DEFAULT);
        }

    }
}
