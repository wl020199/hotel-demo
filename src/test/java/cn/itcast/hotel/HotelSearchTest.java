package cn.itcast.hotel;


import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
public class HotelSearchTest {

    private RestHighLevelClient client;

    @BeforeEach
    void setUp(){
        this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create("http://119.23.69.42:9200")));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

    @Test
    void testMatchAll() throws IOException {

        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchAllQuery());
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        handleResponse(response);
    }

    @Test
    void testMatch() throws IOException {
        SearchRequest request = new SearchRequest("hotel");
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);
    }

    @Test
    void testBoolean() throws IOException {

        SearchRequest request = new SearchRequest("hotel");

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        boolQuery.must(QueryBuilders.termQuery("city","杭州"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(250));

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);

    }

    @Test
    void testPageAndSort() throws IOException {
        // 1.准备请求
        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DsL
        // 2.1 query
        request.source().query(QueryBuilders.matchAllQuery());
        // 2.2 sort
        request.source().sort("price",SortOrder.ASC);
        // 2.3 page
        request.source().from(5).size(5);
        // 3 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);

    }

    @Test
    void testHighLight() throws IOException {

        SearchRequest request = new SearchRequest("hotel");
        // 2.准备DsL
        // 2.1 query
        request.source().query(QueryBuilders.matchQuery("all","如家"));
        // 2.2 sort
        request.source().highlighter(new HighlightBuilder().field("name").requireFieldMatch(false));
        // 2.3 page
        // 3 发送请求
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        handleResponse(response);


    }

    private void handleResponse(SearchResponse response) {
        // 解析response
        SearchHits searchHits = response.getHits();
        // 获取total
        long total = searchHits.getTotalHits().value;
        // 获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 遍历文档数组
        for (SearchHit hit : hits) {
            // 获取jsonString,转实体对象
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);

            // 获取高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();

            if (!CollectionUtils.isEmpty(highlightFields)){
                // 获取高亮字段
                HighlightField highlightField = highlightFields.get("name");
                if (highlightField != null){
                    // 获取高亮值
                    String name = highlightField.getFragments()[0].toString();
                    hotelDoc.setName(name);
                }

            }


            System.out.println(hotelDoc);
        }
    }
}
