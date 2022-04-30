package me.youzheng.springbatch.service;

import java.io.IOException;
import java.util.List;
import me.youzheng.springbatch.batch.domain.ApiInfo;
import me.youzheng.springbatch.batch.domain.ApiRequestVo;
import me.youzheng.springbatch.batch.domain.ApiResponseVo;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

public abstract class AbstractApiService implements ApiService {

    @Override
    public ApiResponseVo service(List<? extends ApiRequestVo> requests) {
        RestTemplateBuilder builder = new RestTemplateBuilder();
        RestTemplate restTemplate = builder.errorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse response) throws IOException {
                return false;
            }

            @Override
            public void handleError(ClientHttpResponse response) throws IOException {

            }
        }).build();

        restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        ApiInfo apiInfo = ApiInfo.builder()
            .apiRequests(requests).build();

        return doApiService(restTemplate, apiInfo);
    }

    protected abstract ApiResponseVo doApiService(RestTemplate restTemplate, ApiInfo apiInfo);
}
