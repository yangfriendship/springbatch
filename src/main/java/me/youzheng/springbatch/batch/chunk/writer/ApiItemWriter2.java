package me.youzheng.springbatch.batch.chunk.writer;

import java.util.List;
import lombok.RequiredArgsConstructor;
import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import me.youzheng.springbatch.batch.domain.ApiResponseVo;
import me.youzheng.springbatch.service.ApiService;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;

public class ApiItemWriter2 extends FlatFileItemWriter<ApiRequestVo> {

    private final ApiService apiService;

    public ApiItemWriter2(ApiService apiService) {
        this.apiService = apiService;
        super.setResource(new FileSystemResource(
            "/Users/youzheng/workspace/study/batch/springbatch/src/main/resources/dist/product2.txt"));
        super.open(new ExecutionContext());
        super.setLineAggregator(new DelimitedLineAggregator<>());
        super.setAppendAllowed(true);
    }

    @Override
    public void write(List<? extends ApiRequestVo> items) throws Exception {
        ApiResponseVo responseVo = apiService.service(items);

        items.stream().forEach(item -> item.setApiResponseVo(responseVo));

        System.out.println("responseVo = " + responseVo);
        super.write(items);
    }
}