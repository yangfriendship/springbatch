package me.youzheng.springbatch.reader.flatfile;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.LineTokenizer;

@RequiredArgsConstructor
public class DefaultLineMapper<T> implements LineMapper<T> {

    private final LineTokenizer lineTokenizer;
    private final FieldSetMapper<T> fieldSetMapper;

    @Override
    public T mapLine(String line, int lineNumber) throws Exception {
        FieldSet tokenize = lineTokenizer.tokenize(line);
        return this.fieldSetMapper.mapFieldSet(tokenize);
    }

}