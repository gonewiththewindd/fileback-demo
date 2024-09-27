package org.gone.file.reconstruct;

import lombok.Data;
import lombok.experimental.Accessors;
import org.gone.file.RollingChecksum;

@Data
@Accessors(chain = true)
public class MatchedFileBlock extends ReconstructFileBlock {
    String md5;
    String id;
    RollingChecksum blockChecksum;
}
