package me.youzheng.springbatch.batch.chunk.writer;

import java.util.List;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import me.youzheng.springbatch.batch.domain.ApiResponseVo;
import me.youzheng.springbatch.service.AbstractApiService;
import me.youzheng.springbatch.service.ApiService;
import org.springframework.batch.item.ItemWriter;

@RequiredArgsConstructor
public class ApiItemWriter1 implements ItemWriter<ApiRequestVo> {

    private final ApiService apiService;

    @Override
    public void write(List<? extends ApiRequestVo> items) throws Exception {
        ApiResponseVo responseVo = apiService.service(items);
        System.out.println("responseVo = " + responseVo);
    }
}
