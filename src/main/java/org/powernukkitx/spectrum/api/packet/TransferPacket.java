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

public class TransferPacket extends Packet {
    @Override
    protected int getId() {
        return PacketIds.TRANSFER;
    }

    public String address;
    public String username;

    @Override
    protected void decode(HandleByteBuf stream) {
        this.address = stream.readString();
        this.username = stream.readString();
    }

    @Override
    protected void encode(HandleByteBuf stream) {
        stream.writeString(this.address);
        stream.writeString(this.username);
    }

    public static TransferPacket create(String address, String username) {
        TransferPacket packet = new TransferPacket();
        packet.address = address;
        packet.username = username;
        return packet;
    }
}
