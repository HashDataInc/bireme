package cn.hashdata.bireme;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public  class Column {
    private String type;
    private String name;
    private boolean signed;
    private String  charset;
}