package cn.itcast.hotel;


import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import java.io.IOException;
import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelIndexTest {

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
    void testInit(){

        System.out.println(restHighLevelClient);
    }


    @Test
    void testCreatHotelIndex() throws IOException {
        // 1.创建request对象
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("hotel");
        // 2.准备请求参数Dsl语句
        createIndexRequest.source(MAPPING_TEMPLATE, XContentType.JSON);
        // 3.发送请求
        restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }
}
