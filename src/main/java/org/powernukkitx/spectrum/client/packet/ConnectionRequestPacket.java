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
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConnectionRequestPacket extends ProxyPacket {

    public String address;
    public String clientData;
    public String identityData;
    public int protocol;
    public String cache;

    @Override
    public int getId() {
        return ProxyPacketIds.CONNECTION_REQUEST;
    }

    @Override
    protected void decodePayload(ByteBuf stream) {
        this.address = readString(stream);
        this.clientData = readString(stream);
        this.identityData = readString(stream);
        this.protocol = stream.readIntLE();
        this.cache = readString(stream);
    }

    @Override
    protected void encodePayload(ByteBuf stream) {
        writeString(stream, this.getAddress());
        writeString(stream, this.getClientData());
        writeString(stream, this.getIdentityData());
        stream.writeIntLE(this.getProtocol());
        writeString(stream, this.getCache());
    }
}
