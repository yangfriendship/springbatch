package me.youzheng.springbatch.batch.chunk.processor;

import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import me.youzheng.springbatch.batch.domain.ProductVo;
import org.springframework.batch.item.ItemProcessor;

public class ApiItemProcessor2 implements ItemProcessor<ProductVo, ApiRequestVo> {
    @Override
    public ApiRequestVo process(ProductVo item) throws Exception {
        return ApiRequestVo.builder()
            .id(item.getId())
            .productVo(item)
            .build();
    }

}