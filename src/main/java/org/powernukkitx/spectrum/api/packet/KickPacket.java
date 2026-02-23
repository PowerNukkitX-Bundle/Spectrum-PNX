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

public class KickPacket extends Packet {
    @Override
    protected int getId() {
        return PacketIds.KICK;
    }

    public String reason;
    public String username;

    @Override
    protected void decode(HandleByteBuf stream) {
        this.reason = stream.readString();
    }

    @Override
    protected void encode(HandleByteBuf stream) {
        stream.writeString(this.reason);
    }

    public static KickPacket create(String reason, String username) {
        KickPacket packet = new KickPacket();
        packet.reason = reason;
        packet.username = username;
        return packet;
    }
}
