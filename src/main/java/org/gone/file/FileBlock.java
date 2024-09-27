package org.gone.file;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class FileBlock {
    String id;
    int index;
    
    private int from;
    private int to;

    RollingChecksum checksum;
    String md5;
    byte[] content;
}
