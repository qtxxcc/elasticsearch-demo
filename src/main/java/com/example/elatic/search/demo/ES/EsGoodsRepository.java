package com.example.elatic.search.demo.ES;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface EsGoodsRepository extends ElasticsearchRepository<EsGoodsEntity, Long> {

    /**
     * 根据goodsId区间查询
     */
    List<EsGoodsEntity> findByGoodsIdBetween(Integer min, Integer max);

}