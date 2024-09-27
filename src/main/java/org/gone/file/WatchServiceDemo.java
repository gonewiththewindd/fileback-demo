package org.gone.file;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;

public class WatchServiceDemo {


    public static void main(String[] args) {
        try {
            WatchService watchService = FileSystems.getDefault().newWatchService();
            Path path = Paths.get("C:\\Users\\gongwenwei\\Desktop");
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE);

            for (; ; ) {
                WatchKey watchKey = watchService.take();
                List<WatchEvent<?>> watchEvents = watchKey.pollEvents();

                watchEvents.forEach(we -> {
                    /**
                     * a.创建文件的时候带有内容会先后触发create, delete, create, modify，事件触发不对
                     * b.修改保存的时候回触发多个modify事件，这个触发次数不对，这对这种情况对于同一文件同一类型的事件可以进行合并后处理
                     * c.重命名操作触发 delete, create
                     */
                    System.out.println("context:" + we.context());// 文件名
                    System.out.println("kind:" + we.kind());// 事件类型
                    System.out.println("count:" + we.count());// 事件类型
//                    WatchEvent.Kind<?> kind = we.kind();
//                    System.out.println(kind.name());
                });
                if(!watchKey.reset()){
                    System.out.println("reset failed");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
