package org.gone.file;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.tomcat.util.buf.ByteBufferUtils;
import org.gone.file.reconstruct.MatchedFileBlock;
import org.gone.file.reconstruct.ModifyByteSerial;
import org.gone.file.reconstruct.ReconstructFileBlock;
import org.springframework.util.DigestUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 滚动校验和：
 * 序列a(k+1,l+1)的校验和可以通过a(k, l)、x(k)、x(l+1)计算获得
 */
@Slf4j
public class RollingCheckSumVersionC {

    /**
     * 文件块大小划分
     */
    public static final int BLOCK_SIZE = 1024;

    public static final int M_1 = (1 << 16) - 1;
    public static final int M = 1 << 16;

    public static final String BASIS_FILE_PATH = "D:\\Users\\Desktop\\不动产—培训资料（20240903）\\不动产—培训资料（20240903）\\04 汇报PPT\\河北雄安新区不动产登记信息管理平台工作汇报-SQ改 - 副本.ppt";
    public static final String DELTA_FILE_PATH = "D:\\Users\\Desktop\\不动产—培训资料（20240903）\\不动产—培训资料（20240903）\\04 汇报PPT\\河北雄安新区不动产登记信息管理平台工作汇报-SQ改.ppt";

//    public static final String BASIS_FILE_PATH = "D:\\Users\\Downloads\\Downloads - 副本.rar";
//    public static final String DELTA_FILE_PATH = "D:\\Users\\Downloads\\Downloads.rar";


    public static final AtomicInteger matchedCnt = new AtomicInteger();

    public static final AtomicInteger md5ComputeCnt = new AtomicInteger();
    public static final AtomicInteger md5ComputeTime = new AtomicInteger();
    public static final AtomicInteger md5ComputeInVainCnt = new AtomicInteger();


    public static final int PROCESS_SIZE_PER_THREAD = 1024 * 1024 * 50;

    public static final int CORES = Runtime.getRuntime().availableProcessors();

    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(CORES);

    public static void main(String[] args) throws IOException {
        FileItem basisFile = splitFile(Paths.get(BASIS_FILE_PATH));
//        byte[] deltaBytes = loadBytes(Paths.get(DELTA_FILE_PATH));
        Path deltaPath = Paths.get(DELTA_FILE_PATH);
        List<ReconstructFileBlock> reconstructFileBlocks = matchFile(deltaPath, basisFile);
        Path reconstructPath = reconstructFile(DELTA_FILE_PATH, basisFile, reconstructFileBlocks);
        logResult(reconstructPath, deltaPath);

//        storeAsFile(reconstructed, DELTA_FILE_PATH);
//        storeAsFile(raw, DELTA_FILE_PATH);
    }

    private static void logResult(Path reconstructed, Path deltaPath) throws IOException {
        String reconstructFileMd5 = DigestUtils.md5DigestAsHex(Files.newInputStream(reconstructed));
        System.out.println(String.format("delta file md5:%s, reconstruct file md5:%s", DigestUtils.md5DigestAsHex(Files.newInputStream(deltaPath)), reconstructFileMd5));
        System.out.println(String.format("md5 compute cnt:%s, in vain cnt:%s, compute time:%s ms", md5ComputeCnt.get(), md5ComputeInVainCnt, md5ComputeTime.get()));
    }

    private static Path reconstructFile(String deltaFilePath, FileItem basisFile, List<ReconstructFileBlock> reconstructFileBlocks) {
        Map<String, FileBlock> fileBlockMap = basisFile.getFileBlockMap()
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(FileBlock::getId, v -> v));

        String extension = FilenameUtils.getExtension(deltaFilePath);
        String filename = UUID.randomUUID().toString();
        try {
            Path tempFile = Files.createTempFile(filename, "." + extension);
            try (OutputStream bufferedWriter = Files.newOutputStream(tempFile)) {
                for (ReconstructFileBlock fb : reconstructFileBlocks) {
                    if (fb instanceof ModifyByteSerial) {
                        bufferedWriter.write(((ModifyByteSerial) fb).getContent());
                    } else {
                        bufferedWriter.write(fileBlockMap.get(((MatchedFileBlock) fb).getId()).getContent());
                    }
                }
            }
            System.out.println(tempFile.toAbsolutePath());
            return tempFile.toAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 两种算法进行匹配：
     * 1.低计算成本的滚动校验和
     * 2.低冲突的算法：数字摘要
     *
     * @param deltaPath
     * @param basisFile
     * @return
     */
    private static List<ReconstructFileBlock> matchFile(Path deltaPath, FileItem basisFile) {
        // 并发搜索，各线程负责区域切分，提交任务
        long start = System.currentTimeMillis();
//        List<CompletableFuture<List<ReconstructFileBlock>>> futures = new ArrayList<>();
//        for (int from = 0; from < deltaBytes.length; from += PROCESS_SIZE_PER_THREAD) {
//            int to = from + PROCESS_SIZE_PER_THREAD;
//            if (to > deltaBytes.length) {
//                to = deltaBytes.length;
//            }
//            int finalFrom = from;
//            int finalTo = to;
//            CompletableFuture<List<ReconstructFileBlock>> matchTaskFuture = CompletableFuture.supplyAsync(() -> doSearch(deltaBytes, finalFrom, finalTo, basisFile), EXECUTOR);
//            futures.add(matchTaskFuture);
//        }
//        // 搜索任务同步等待
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
//        // 重构文件排序
//        List<ReconstructFileBlock> fileBlocks = futures.stream()
//                .map(f -> {
//                    try {
//                        return f.get();
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    } catch (ExecutionException e) {
//                        throw new RuntimeException(e);
//                    }
//                })
//                .flatMap(Collection::stream)
//                .sorted(Comparator.comparing(ReconstructFileBlock::getFrom))
//                .collect(Collectors.toList());

        List<ReconstructFileBlock> fileBlocks = doSearch(deltaPath, basisFile);
        System.out.println(String.format("match: %s, end in %s ms", matchedCnt.get(), System.currentTimeMillis() - start));
        return fileBlocks;
    }

    /**
     * 文件块匹配搜索
     *
     * @param deltaPath
     * @param basisFile
     * @return
     */
    private static List<ReconstructFileBlock> doSearch(Path deltaPath, FileItem basisFile) {
        long startTime = System.currentTimeMillis();
        List<ReconstructFileBlock> reconstructList = new ArrayList<>();
        RollingChecksum lastChecksum = null;
        int modifyBegin = -1;
        ByteBuffer changeBuffer = ByteBuffer.allocate(1024 * 1024);
        // 有没有可能多线程并非搜索，难点在于线程负责的区域不可重叠，
        // 这样可能出现的情况就是线程和线程相邻的区域割裂，导致潜在的命中块降低，最坏的情况就是N个块都属于命中块
        long length = deltaPath.toFile().length();
        int bufferSize = BLOCK_SIZE * 1024;
        byte[] buffer = loadBytes(deltaPath, 0, bufferSize);
        int bufferOffset = 0;// 当前缓冲区在文件中的偏移量
        int current = 0;// 当前文件的比较字节下标, buffer的索引下标 =  current - bufferOffset
        for (; current < length - BLOCK_SIZE; ) {
            int bufferIndex = current - bufferOffset;
            if (bufferIndex > bufferSize - BLOCK_SIZE) {
                buffer = loadBytes(deltaPath, current - 1, bufferSize);
                bufferOffset = current - 1;
            }
            SearchFileBlock searchFileBlock = searchFileBlock(buffer, current, bufferOffset, basisFile, lastChecksum);
            if (searchFileBlock.isMatched()) {
                // 之前缓存的未匹配的连续修改字节先处理
                if (changeBuffer.position() > 0) {
                    ReconstructFileBlock reconstructFileBlock = generateModifyByteSerial(changeBuffer, modifyBegin, current);
                    reconstructList.add(reconstructFileBlock);
                    modifyBegin = -1;
                }
                // 处理匹配的块
                reconstructList.add(searchFileBlock.getMatchedFileBlock());
                current += BLOCK_SIZE;
                lastChecksum = null;
                matchedCnt.getAndIncrement();
            } else {
                // 合并连续字节修改序列
                if (modifyBegin < 0) {
                    modifyBegin = current; // 记录连续修改字节起始位置
                }
                if (changeBuffer.position() >= changeBuffer.capacity()) { // 扩容
                    changeBuffer = ByteBufferUtils.expand(changeBuffer, changeBuffer.capacity() * 2);
                }
                changeBuffer.put(buffer[current - bufferOffset]);
                lastChecksum = searchFileBlock.getLastChecksum();
                current++;
            }
        }
        if (current < length) {
            if (modifyBegin < 0) {//文件最后不存在连续修改内容，修改起始位置重置为当前位置
                modifyBegin = current;
            }
            byte[] rest = loadBytes(deltaPath, current, (int) length - current);
            changeBuffer.put(rest);
        }
        // 可能存在最后未匹配未处理的缓存
        if (changeBuffer.position() > 0) {
            ReconstructFileBlock reconstructFileBlock = generateModifyByteSerial(changeBuffer, modifyBegin, (int) length);
            reconstructList.add(reconstructFileBlock);
        }

        System.out.println(String.format("search from byte %s to %s, end in %s ms", 0, length, System.currentTimeMillis() - startTime));
        return reconstructList;
    }

    private static byte[] loadBytes(Path deltaPath, int current, byte[] oldBuffer, int bufferIndex) {
//        byte[] rest = Arrays.copyOfRange(oldBuffer, bufferIndex, oldBuffer.length);
        byte[] newBuffer = new byte[oldBuffer.length];
        System.arraycopy(oldBuffer, bufferIndex, newBuffer, 0, oldBuffer.length - bufferIndex);
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(Files.newInputStream(deltaPath))) {
            bufferedInputStream.skip(current);
            bufferedInputStream.read(newBuffer, oldBuffer.length - bufferIndex, bufferIndex);
            return newBuffer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] loadBytes(Path deltaPath, int current, int length) {
        byte[] buffer = new byte[length];
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(Files.newInputStream(deltaPath))) {
            bufferedInputStream.skip(current);
            bufferedInputStream.read(buffer, 0, length);
            return buffer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ReconstructFileBlock generateModifyByteSerial(ByteBuffer byteBuffer, int from, int to) {

        byte[] bytes = new byte[byteBuffer.position()];
        byteBuffer.rewind();
        byteBuffer.get(bytes);
        byteBuffer.clear();

        return new ModifyByteSerial().setContent(bytes).setFrom(from).setTo(to);
    }

    private static SearchFileBlock searchFileBlock(byte[] bytes, int from, int bufferOffset, FileItem basisFile, RollingChecksum lastChecksum) {
        int to = from + BLOCK_SIZE;
        RollingChecksum checksum;
        if (Objects.isNull(lastChecksum)) { // 命中匹配块以后重新计算校验和
            checksum = checksum(bytes, from, BLOCK_SIZE, bufferOffset);
        } else {// 其他情况通过滚动校验和计算下一个块校验和
            checksum = nextBlockCheckSum(bytes, to - 1, from - 1, bufferOffset, lastChecksum);
        }
        Map<Integer, List<FileBlock>> checksumMap = basisFile.getFileBlockMap();
        SearchFileBlock searchFileBlock = new SearchFileBlock().setLastChecksum(checksum);
        if (checksumMap.containsKey(checksum.getS())) {
            // 滚动校验和匹配的情况下，校验md5是否匹配
            byte[] targetBytes = Arrays.copyOfRange(bytes, from - bufferOffset, to - bufferOffset);
            String targetMd5 = traceMd5Compute(targetBytes);
            List<FileBlock> fileBlocks = checksumMap.get(checksum.getS());
            Optional<FileBlock> targetFileBlock = fileBlocks.stream()
                    .filter(fileBlock -> /*quickMatch(fileBlock, targetBytes) &&*/ fileBlock.getMd5().equalsIgnoreCase(targetMd5))
                    .findFirst();
            if (targetFileBlock.isPresent()) {
                MatchedFileBlock matchedFileBlock = new MatchedFileBlock()
                        .setId(targetFileBlock.get().getId())
                        .setBlockChecksum(checksum);
                matchedFileBlock.setFrom(from);
                matchedFileBlock.setTo(to);

                searchFileBlock.setMatchedFileBlock(matchedFileBlock);
                searchFileBlock.setLastChecksum(null);
                return searchFileBlock;
            } else {
                md5ComputeInVainCnt.getAndIncrement();
            }
        }

        return searchFileBlock;
    }

    private static boolean quickMatch(FileBlock fileBlock, byte[] targetBytes) {
        for (int i = 0; i < 10; i++) {
            if (fileBlock.getContent()[i] != targetBytes[i]) {
                return false;
            }
        }
        return true;
    }

    private static String traceMd5Compute(byte[] targetBytes) {
        long start = System.currentTimeMillis();
        String targetMd5 = DigestUtils.md5DigestAsHex(targetBytes);
        md5ComputeTime.addAndGet((int) (System.currentTimeMillis() - start));
        md5ComputeCnt.getAndIncrement();
        return targetMd5;
    }

    public static RollingChecksum checksum(byte[] buffer, int offset, int length, int bufferOffset) {
        int suma = 0, l = offset + length - 1;
        for (int i = offset; i <= l; i++) {
            suma += buffer[i - bufferOffset];
        }
        int a = suma & M_1;

        int sumb = 0;
        for (int i = offset; i <= l; i++) {
            sumb += (l - i + 1) * buffer[i - bufferOffset];
        }
        int b = sumb & M_1;

        int s = a + M * b;

        return new RollingChecksum(a, b, s);
    }

    public static RollingChecksum nextBlockCheckSum(byte[] bytes, int l, int k, int bufferOffset, RollingChecksum checksum) {

        try {
            int an = (checksum.getA() - bytes[k - bufferOffset] + bytes[l - bufferOffset]) & M_1;
            int bn = (checksum.getB() - (l - k) * bytes[k - bufferOffset] + an) & M_1;
            int sn = an + M * bn;

            return new RollingChecksum(an, bn, sn);
        } catch (Exception e) {
            log.error("l:{}, k:{}, bufferOffset:{}", l, k, bufferOffset);
            throw new RuntimeException(e);
        }
    }

    /**
     * 文件按照BLOCK_SIZE进行切割，并记录起始位置和结束位置，计算校验和、MD5
     *
     * @param path
     * @return
     */
    public static FileItem splitFile(Path path) {
        long start = System.currentTimeMillis();
        byte[] bytes = loadBytes(path);
        List<FileBlock> fileBlocks = new ArrayList<>(bytes.length / BLOCK_SIZE + 1);
        for (int from = 0, to, index = 0; from < bytes.length; from += BLOCK_SIZE) {
            to = from + BLOCK_SIZE;
            if (to > bytes.length) {
                to = bytes.length;
            }
            byte[] content = Arrays.copyOfRange(bytes, from, to);
            FileBlock fileBlock = new FileBlock()
                    .setId(UUID.randomUUID().toString())
                    .setIndex(index);
            fileBlock.setFrom(from);
            fileBlock.setTo(to);
            fileBlock.setChecksum(checksum(bytes, from, content.length, 0));
            fileBlock.setMd5(DigestUtils.md5DigestAsHex(content));
            fileBlock.setContent(content);
            fileBlocks.add(fileBlock);
        }
        Map<Integer, List<FileBlock>> fileBlockMap = fileBlocks.stream().collect(Collectors.groupingBy(v -> v.getChecksum().getS()));
        System.out.println(String.format("load and init basis file end in %s ms", System.currentTimeMillis() - start));
        return new FileItem()
                .setPath(path)
                .setMd5(DigestUtils.md5DigestAsHex(bytes))
                .setFileBlockMap(fileBlockMap);
    }

    private static byte[] loadBytes(Path path) {
        try {
            return IOUtils.readFully(Files.newInputStream(path), (int) path.toFile().length());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
