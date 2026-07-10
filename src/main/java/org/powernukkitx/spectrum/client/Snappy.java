package org.powernukkitx.spectrum.client;

import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;

/**
 * Raw (block-format) snappy, matching Go's snappy.Encode and PHP ext-snappy used by Spectrum.
 */
public final class Snappy {
    private static final ThreadLocal<SnappyCompressor> COMPRESSOR = ThreadLocal.withInitial(SnappyCompressor::new);
    private static final ThreadLocal<SnappyDecompressor> DECOMPRESSOR = ThreadLocal.withInitial(SnappyDecompressor::new);

    private Snappy() {
    }

    public static byte[] compress(byte[] input) {
        SnappyCompressor compressor = COMPRESSOR.get();
        byte[] output = new byte[compressor.maxCompressedLength(input.length)];
        int size = compressor.compress(input, 0, input.length, output, 0, output.length);
        byte[] result = new byte[size];
        System.arraycopy(output, 0, result, 0, size);
        return result;
    }

    public static byte[] decompress(byte[] input, int offset, int length) {
        int uncompressed = SnappyDecompressor.getUncompressedLength(input, offset);
        byte[] output = new byte[uncompressed];
        DECOMPRESSOR.get().decompress(input, offset, length, output, 0, output.length);
        return output;
    }
}
