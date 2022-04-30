package me.youzheng.springbatch.batch.classsifier;

import java.util.HashMap;
import java.util.Map;
import lombok.Setter;
import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import org.springframework.batch.item.ItemWriter;
import org.springframework.classify.Classifier;

public class WriterClassifier<C, T> implements Classifier<C, T> {

    @Setter
    private Map<String, ItemWriter<ApiRequestVo>> map = new HashMap<>();

    @Override
    public T classify(C classifiable) {
        return (T) this.map.get(((ApiRequestVo) classifiable).getProductVo().getType());
    }
}
