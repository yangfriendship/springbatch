package me.youzheng.springbatch.batch.chunk.processor;

import me.youzheng.springbatch.batch.domain.Product;
import me.youzheng.springbatch.batch.domain.ProductVo;
import org.springframework.batch.item.ItemProcessor;

public class FileItemProcessor implements ItemProcessor<ProductVo, Product> {

    @Override
    public Product process(ProductVo item) throws Exception {
        return new Product(item.getId(),item.getName(), item.getPrice(), item.getType());
    }
}
