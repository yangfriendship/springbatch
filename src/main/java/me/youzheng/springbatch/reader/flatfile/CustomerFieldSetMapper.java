package me.youzheng.springbatch.reader.flatfile;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class CustomerFieldSetMapper implements FieldSetMapper<Customer> {

    @Override
    public Customer mapFieldSet(FieldSet fieldSet) throws BindException {
        if (fieldSet == null) {
            return null;
        }
        Customer result = new Customer();
//        result.setName(fieldSet.readString(0));
//        result.setAge(fieldSet.readString(1));
//        result.setYear(fieldSet.readString(2));

        result.setName(fieldSet.readString("name"));
        result.setAge(fieldSet.readString("age"));
        result.setYear(fieldSet.readString("year"));
        return result;
    }
}
