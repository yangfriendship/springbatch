package me.youzheng.springbatch.batch.classsifier;

import java.util.HashMap;
import java.util.Map;
import lombok.Setter;
import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import me.youzheng.springbatch.batch.domain.ProductVo;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.classify.Classifier;

public class ProcessorClassifier<C, T> implements Classifier<C, T> {

    @Setter
    private Map<String, ItemProcessor<ProductVo, ApiRequestVo>> map = new HashMap<>();

    @Override
    public T classify(C classifiable) {
        return (T) this.map.get(((ProductVo) classifiable).getType());
    }
}
