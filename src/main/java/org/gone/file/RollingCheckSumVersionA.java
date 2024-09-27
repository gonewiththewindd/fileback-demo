//package com.gone.file_backup.file;
//
//import com.gone.file_backup.file.reconstruct.MatchedFileBlock;
//import com.gone.file_backup.file.reconstruct.ModifyByteSerial;
//import com.gone.file_backup.file.reconstruct.ReconstructFileBlock;
//import org.apache.commons.io.FilenameUtils;
//import org.apache.commons.io.IOUtils;
//import org.apache.tomcat.util.buf.ByteBufferUtils;
//import org.springframework.util.DigestUtils;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.OutputStream;
//import java.nio.ByteBuffer;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Collectors;
//
///**
// * 滚动校验和：
// * 序列a(k+1,l+1)的校验和可以通过a(k, l)、x(k)、x(l+1)计算获得
// */
//public class RollingCheckSumVersionA {
//
//    /**
//     * 文件块大小划分
//     */
//    public static final int BLOCK_SIZE = 4096;
//
//    public static final int M = (1 << 16) - 1;
//
//    public static final String BASIS_FILE_PATH = "D:\\Users\\Downloads\\Downloads - 副本.rar";
//    public static final String DELTA_FILE_PATH = "D:\\Users\\Downloads\\Downloads.rar";
//
//    public static final AtomicInteger matchedCnt = new AtomicInteger();
//
//    public static final AtomicInteger md5ComputeCnt = new AtomicInteger();
//    public static final AtomicInteger md5ComputeTime = new AtomicInteger();
//    public static final AtomicInteger md5ComputeInVainCnt = new AtomicInteger();
//
//
//    public static final int PROCESS_SIZE_PER_THREAD = 1024 * 1024 * 50;
//
//    public static final int CORES = Runtime.getRuntime().availableProcessors();
//
//    public static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(CORES);
//
//    public static void main(String[] args) {
//        FileItem basisFile = splitFile(Paths.get(BASIS_FILE_PATH));
//        byte[] deltaBytes = loadBytes(Paths.get(DELTA_FILE_PATH));
//        List<ReconstructFileBlock> reconstructFileBlocks = matchFile(deltaBytes, basisFile);
//        byte[] reconstructed = reconstructFile(basisFile, reconstructFileBlocks);
//        logResult(reconstructed, deltaBytes);
//
//        storeAsFile(reconstructed, DELTA_FILE_PATH);
////        storeAsFile(raw, DELTA_FILE_PATH);
//    }
//
//    private static void logResult(byte[] reconstructed, byte[] raw) {
//        if (reconstructed.length != raw.length) {
//            System.out.println(String.format("reconstructed file length not equal to original, reconstructed:%s, raw:%s", reconstructed.length, raw.length));
//        } else {
//            int incorrect = 0;
//            for (int i = 0; i < raw.length; i++) {
//                if (reconstructed[i] != raw[i]) {
//                    incorrect++;
//                    if (incorrect < 10) {
//                        System.out.println(String.format("position:%s, raw:%s, reconstruct:%s", i, raw[i], reconstructed[i]));
//                    }
//                }
//            }
//            if (incorrect > 0) {
//                System.out.println(String.format("incorrect:%s", incorrect));
//            }
//        }
//        String reconstructFileMd5 = DigestUtils.md5DigestAsHex(reconstructed);
//        System.out.println(String.format("delta file md5:%s, reconstruct file md5:%s", DigestUtils.md5DigestAsHex(raw), reconstructFileMd5));
//        System.out.println(String.format("md5 compute cnt:%s, in vain cnt:%s, compute time:%s ms", md5ComputeCnt.get(), md5ComputeInVainCnt, md5ComputeTime.get()));
//    }
//
//    private static void storeAsFile(byte[] reconstructed, String deltaFilePath) {
//        String extension = FilenameUtils.getExtension(deltaFilePath);
//        String filename = UUID.randomUUID().toString();
//        try {
//            Path tempFile = Files.createTempFile(filename, "." + extension);
//            try (OutputStream bufferedWriter = Files.newOutputStream(tempFile)) {
//                IOUtils.write(reconstructed, bufferedWriter);
//            }
//            System.out.println(tempFile.toAbsolutePath());
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private static byte[] reconstructFile(FileItem basisFile, List<ReconstructFileBlock> reconstructFileBlocks) {
//        Map<String, FileBlock> fileBlockMap = basisFile.getFileBlockMap()
//                .values()
//                .stream()
//                .flatMap(Collection::stream)
//                .collect(Collectors.toMap(FileBlock::getId, v -> v));
//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//        reconstructFileBlocks.stream().map(fb -> {
//            if (fb instanceof ModifyByteSerial) {
//                return ((ModifyByteSerial) fb).getContent();
//            } else {
//                return fileBlockMap.get(((MatchedFileBlock) fb).getId()).getContent();
//            }
//        }).forEach(bytes -> {
//            try {
//                byteArrayOutputStream.write(bytes);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        });
//        return byteArrayOutputStream.toByteArray();
//    }
//
//    /**
//     * 两种算法进行匹配：
//     * 1.低计算成本的滚动校验和
//     * 2.低冲突的算法：数字摘要
//     *
//     * @param deltaBytes
//     * @param basisFile
//     * @return
//     */
//    private static List<ReconstructFileBlock> matchFile(byte[] deltaBytes, FileItem basisFile) {
//        // 并发搜索，各线程负责区域切分，提交任务
//        long start = System.currentTimeMillis();
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
////        List<ReconstructFileBlock> fileBlocks = doSearch(deltaBytes, 0, deltaBytes.length, basisFile);
//        System.out.println(String.format("match: %s, end in %s ms", matchedCnt.get(), System.currentTimeMillis() - start));
//        return fileBlocks;
//    }
//
//    /**
//     * 文件块匹配搜索
//     *
//     * @param deltaBytes
//     * @param from       起始下标
//     * @param to         结束下标
//     * @param basisFile
//     * @return
//     */
//    private static List<ReconstructFileBlock> doSearch(byte[] deltaBytes, int from, int to, FileItem basisFile) {
//        long startTime = System.currentTimeMillis();
//        List<ReconstructFileBlock> reconstructList = new ArrayList<>();
//        int lastChecksum = 0, lastIndex = -1, modifyBegin = -1;
//        ByteBuffer byteBuffer = ByteBuffer.allocate(1024 * 1024);
//        // 有没有可能多线程并非搜索，难点在于线程负责的区域不可重叠，
//        // 这样可能出现的情况就是线程和线程相邻的区域割裂，导致潜在的命中块降低，最坏的情况就是N个块都属于命中块
//        int current = from, currentEnd;
//        for (; current < to - BLOCK_SIZE; ) {
//            currentEnd = current + BLOCK_SIZE;
//            // 搜索匹配的块
//            SearchFileBlock searchFileBlock = searchFileBlock(deltaBytes, current, currentEnd, basisFile, lastIndex, lastChecksum);
//            if (searchFileBlock.isMatched()) {
//                // 之前缓存的未匹配的连续修改字节先处理
//                if (byteBuffer.position() > 0) {
//                    ReconstructFileBlock reconstructFileBlock = generateModifyByteSerial(byteBuffer, modifyBegin, current);
//                    reconstructList.add(reconstructFileBlock);
//                    modifyBegin = -1;
//                }
//                // 处理匹配的块
//                reconstructList.add(searchFileBlock.getMatchedFileBlock());
//                current += BLOCK_SIZE;
//                lastIndex = -1; // 命中清除标记
//                lastChecksum = 0;
//                matchedCnt.getAndIncrement();
//            } else {
//                // 合并连续字节修改序列
//                if (modifyBegin < 0) {
//                    modifyBegin = current; // 记录连续修改字节起始位置
//                }
//                if (byteBuffer.position() >= byteBuffer.capacity()) { // 扩容
//                    byteBuffer = ByteBufferUtils.expand(byteBuffer, byteBuffer.capacity() * 2);
//                }
//                byteBuffer.put(deltaBytes[current]);
//                lastIndex = current;
//                lastChecksum = searchFileBlock.getLastChecksum();
//                current++;
//            }
//        }
//        if (current < to) {
//            if (modifyBegin < 0) {
//                modifyBegin = current;
//            }
//            byteBuffer.put(Arrays.copyOfRange(deltaBytes, current, to));
//        }
//        // 可能存在最后未匹配未处理的缓存
//        if (byteBuffer.position() > 0) {
//            ReconstructFileBlock reconstructFileBlock = generateModifyByteSerial(byteBuffer, modifyBegin, to);
//            reconstructList.add(reconstructFileBlock);
//        }
//
//        System.out.println(String.format("search from byte %s to %s, end in %s ms", from, to, System.currentTimeMillis() - startTime));
//        return reconstructList;
//    }
//
//    private static ReconstructFileBlock generateModifyByteSerial(ByteBuffer byteBuffer, int from, int to) {
//
//        byte[] bytes = new byte[byteBuffer.position()];
//        byteBuffer.rewind();
//        byteBuffer.get(bytes);
//        byteBuffer.clear();
//
//        return new ModifyByteSerial().setContent(bytes).setFrom(from).setTo(to);
//    }
//
//    private static SearchFileBlock searchFileBlock(byte[] bytes, int from, int to, FileItem basisFile, int lastIndex, int lastChecksum) {
//        int length = to - from, checksum;
//        if (lastIndex < 0) { // 命中匹配块以后重新计算校验和
//            checksum = checksum(bytes, from, length);
//        } else {// 其他情况通过滚动校验和计算下一个块校验和
//            checksum = nextBlockCheckSum(lastChecksum, bytes[from - 1], bytes[to]);
//        }
//        Map<Integer, List<FileBlock>> checksumMap = basisFile.getFileBlockMap();
//        SearchFileBlock searchFileBlock = new SearchFileBlock().setLastChecksum(checksum);
//        if (checksumMap.containsKey(checksum)) {
//            // 滚动校验和匹配的情况下，校验md5是否匹配
//            byte[] targetBytes = Arrays.copyOfRange(bytes, from, to);
////            String targetMd5 = traceMd5Compute(targetBytes);
//            List<FileBlock> fileBlocks = checksumMap.get(checksum);
//            Optional<FileBlock> targetFileBlock = fileBlocks.stream()
//                    .filter(fileBlock -> quickMatch(fileBlock, targetBytes) && fileBlock.getMd5().equalsIgnoreCase(traceMd5Compute(targetBytes)))
//                    .findFirst();
//            if (targetFileBlock.isPresent()) {
//                MatchedFileBlock matchedFileBlock = new MatchedFileBlock()
//                        .setId(targetFileBlock.get().getId())
//                        .setBlockChecksum(checksum);
//                matchedFileBlock.setFrom(from);
//                matchedFileBlock.setTo(to);
//
//                searchFileBlock.setMatchedFileBlock(matchedFileBlock);
//                searchFileBlock.setLastChecksum(0);
//                return searchFileBlock;
//            } else {
//                md5ComputeInVainCnt.getAndIncrement();
//            }
//        }
//
//        return searchFileBlock;
//    }
//
//    private static boolean quickMatch(FileBlock fileBlock, byte[] targetBytes) {
//        for (int i = 0; i < 10; i++) {
//            if (fileBlock.getContent()[i] != targetBytes[i]) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//    private static String traceMd5Compute(byte[] targetBytes) {
//        long start = System.currentTimeMillis();
//        String targetMd5 = DigestUtils.md5DigestAsHex(targetBytes);
//        md5ComputeTime.addAndGet((int) (System.currentTimeMillis() - start));
//        md5ComputeCnt.getAndIncrement();
//        return targetMd5;
//    }
//
//    public static int checksum(byte[] raw, int offset, int length) {
//        int sum = 0;
//        for (int i = offset; i < offset + length; i++) {
//            sum += raw[i];
//        }
//        return sum & M;
//    }
//
//    public static int nextBlockCheckSum(int checksum, byte last, byte next) {
//        return (checksum - last + next) & M;
//    }
//
//    /**
//     * 文件按照BLOCK_SIZE进行切割，并记录起始位置和结束位置，计算校验和、MD5
//     *
//     * @param path
//     * @return
//     */
//    public static FileItem splitFile(Path path) {
//        long start = System.currentTimeMillis();
//        byte[] bytes = loadBytes(path);
//        List<FileBlock> fileBlocks = new ArrayList<>(bytes.length / BLOCK_SIZE + 1);
//        for (int from = 0, to, index = 0; from < bytes.length; from += BLOCK_SIZE) {
//            to = from + BLOCK_SIZE;
//            if (to > bytes.length) {
//                to = bytes.length;
//            }
//            byte[] content = Arrays.copyOfRange(bytes, from, to);
//            FileBlock fileBlock = new FileBlock()
//                    .setId(UUID.randomUUID().toString())
//                    .setIndex(index);
//            fileBlock.setFrom(from);
//            fileBlock.setTo(to);
//            fileBlock.setChecksum(checksum(bytes, from, content.length));
//            fileBlock.setMd5(DigestUtils.md5DigestAsHex(content));
//            fileBlock.setContent(content);
//            fileBlocks.add(fileBlock);
//        }
//        Map<Integer, List<FileBlock>> fileBlockMap = fileBlocks.stream().collect(Collectors.groupingBy(FileBlock::getChecksum));
//        System.out.println(String.format("load and init basis file end in %s ms", System.currentTimeMillis() - start));
//        return new FileItem()
//                .setPath(path)
//                .setMd5(DigestUtils.md5DigestAsHex(bytes))
//                .setFileBlockMap(fileBlockMap);
//    }
//
//    private static byte[] loadBytes(Path path) {
//        try {
//            return IOUtils.readFully(Files.newInputStream(path), (int) path.toFile().length());
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//}
