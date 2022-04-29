package me.youzheng.springbatch.web;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobInfo {


    private String id;

    public String getId() {
        return this.id;
    }
}
