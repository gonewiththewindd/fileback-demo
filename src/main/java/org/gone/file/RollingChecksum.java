package org.gone.file;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RollingChecksum {
    private int a;
    private int b;
    private int s;
}
