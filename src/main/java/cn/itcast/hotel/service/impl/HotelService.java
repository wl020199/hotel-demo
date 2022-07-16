package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1.准备request
            SearchRequest request = new SearchRequest("hotel");
            // 2.准备DSL
            // 2.1 query
            // 构建booleanQuery
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            // 关键字搜索
            String key = params.getKey();
            if (key == null || "".equals(key)) {
                boolQuery.must(QueryBuilders.matchAllQuery());
            } else {
                boolQuery.must(QueryBuilders.matchQuery("all", key));
            }
            // 城市条件
            if (params.getCity() != null && !"".equals(params.getCity())){
                boolQuery.filter(QueryBuilders.termQuery("city",params.getCity()));
            }
            // 品牌条件
            if (params.getBrand() != null && !"".equals(params.getBrand())){
                boolQuery.filter(QueryBuilders.termQuery("brand",params.getBrand()));
            }
            // 星级条件
            if (params.getStarName() != null && !"".equals(params.getStarName())){
                boolQuery.filter(QueryBuilders.termQuery("starName",params.getStarName()));
            }
            // 价格条件
            if (params.getMaxPrice() != null && params.getMinPrice() != null){
                boolQuery.filter(QueryBuilders.rangeQuery("price").gte(params.getMinPrice()).lte(params.getMaxPrice()));
            }

            FunctionScoreQueryBuilder functionScoreQuery = QueryBuilders.functionScoreQuery(
                    boolQuery,
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                            new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                    QueryBuilders.termQuery("isAD", "true"),
                                    ScoreFunctionBuilders.weightFactorFunction(10)
                            )
                    });

            request.source().query(functionScoreQuery);


            // 2.2 page
            int page = params.getPage();
            int size = params.getSize();
            request.source().from((page - 1) * size).size(size);
            // 2.3 排序
            String location = params.getLocation();
            if (location != null && !"".equals(location)){
                request.source().sort(SortBuilders
                        .geoDistanceSort("location",new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS));
            }

            // 3.发送请求,得到响应
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);
            // 4.解析响应
            return handleResponse(response);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private PageResult handleResponse(SearchResponse response) {
        // 解析response
        SearchHits searchHits = response.getHits();
        // 获取total
        long total = searchHits.getTotalHits().value;
        // 获取文档数组
        SearchHit[] hits = searchHits.getHits();
        // 准备结果集合
        List<HotelDoc> hotels = new ArrayList<>();
        // 遍历文档数组
        for (SearchHit hit : hits) {
            // 获取jsonString,转实体对象
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
            Object[] sortValues = hit.getSortValues();
            if (sortValues.length > 0){
                hotelDoc.setDistance(sortValues[0]);
            }
            hotels.add(hotelDoc);
        }
        return new PageResult(total,hotels);
    }
}
