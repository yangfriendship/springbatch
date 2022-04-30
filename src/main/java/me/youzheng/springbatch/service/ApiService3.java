package me.youzheng.springbatch.service;

import me.youzheng.springbatch.batch.domain.ApiInfo;
import me.youzheng.springbatch.batch.domain.ApiResponseVo;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ApiService3 extends AbstractApiService {

    @Override
    protected ApiResponseVo doApiService(RestTemplate restTemplate, ApiInfo apiInfo) {
        ResponseEntity<String> responseEntity = restTemplate.postForEntity(
            "http://localhost:8083/api/product/3", apiInfo, String.class);
        int statusCodeValue = responseEntity.getStatusCodeValue();
        ApiResponseVo responseVo = ApiResponseVo.builder()
            .status(statusCodeValue)
            .message(responseEntity.getBody())
            .build();
        return responseVo;
    }
}
