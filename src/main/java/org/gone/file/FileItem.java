package org.gone.file;

import lombok.Data;
import lombok.experimental.Accessors;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@Data
@Accessors(chain = true)
public class FileItem {
    Path path;
    String md5;
    Map<Integer, List<FileBlock>> fileBlockMap;
}
