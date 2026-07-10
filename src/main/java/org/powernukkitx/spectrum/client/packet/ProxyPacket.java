package org.powernukkitx.spectrum.client.packet;

/*
  MIT License

  Copyright (c) 2024 cooldogedev

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in all
  copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
  SOFTWARE.

  @auto-license
 */

import io.netty.buffer.ByteBuf;
import org.cloudburstmc.protocol.common.util.VarInts;

import java.nio.charset.StandardCharsets;

public abstract class ProxyPacket {
    public static final int PID_MASK = 0x3ff;

    public abstract int getId();

    protected abstract void decodePayload(ByteBuf stream);
    protected abstract void encodePayload(ByteBuf stream);

    public final void decode(ByteBuf stream) {
        VarInts.readUnsignedInt(stream);
        this.decodePayload(stream);
    }

    public final void encode(ByteBuf stream) {
        VarInts.writeUnsignedInt(stream, this.getId());
        this.encodePayload(stream);
    }

    protected String readString(ByteBuf stream) {
        byte[] bytes = new byte[VarInts.readUnsignedInt(stream)];
        stream.readBytes(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    protected void writeString(ByteBuf stream, String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        VarInts.writeUnsignedInt(stream, bytes.length);
        stream.writeBytes(bytes);
    }
}
