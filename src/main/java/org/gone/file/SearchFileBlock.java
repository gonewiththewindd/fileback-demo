package org.gone.file;

import lombok.Data;
import lombok.experimental.Accessors;
import org.gone.file.reconstruct.MatchedFileBlock;

import java.util.Objects;

@Data
@Accessors(chain = true)
public class SearchFileBlock {

    private RollingChecksum lastChecksum;
    private MatchedFileBlock matchedFileBlock;

    public boolean isMatched() {
        return Objects.nonNull(matchedFileBlock);
    }
}
