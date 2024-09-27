package org.gone.file.reconstruct;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ModifyByteSerial extends ReconstructFileBlock {
    private byte[] content;
}
