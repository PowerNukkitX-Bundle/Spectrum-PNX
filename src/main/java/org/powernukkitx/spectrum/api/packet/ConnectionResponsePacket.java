package org.powernukkitx.spectrum.api.packet;

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

import cn.nukkit.network.connection.util.HandleByteBuf;
import lombok.Getter;
import lombok.Setter;

public class ConnectionResponsePacket extends Packet {
    public static int RESPONSE_SUCCESS = 0;
    public static int RESPONSE_UNAUTHORIZED = 1;
    public static int RESPONSE_FAIL = 2;

    @Getter
    @Setter
    public int response;

    @Override
    protected int getId() {
        return PacketIds.CONNECTION_RESPONSE;
    }

    @Override
    protected void decode(HandleByteBuf stream) {
        this.response = stream.readByte();
    }

    @Override
    protected void encode(HandleByteBuf stream) {
        stream.writeByte(this.getResponse());
    }
}
