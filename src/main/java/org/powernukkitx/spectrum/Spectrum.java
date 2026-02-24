package org.powernukkitx.spectrum;

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

import cn.nukkit.plugin.PluginBase;
import cn.nukkit.utils.ConfigSection;
import org.powernukkitx.spectrum.api.APIThread;
import org.powernukkitx.spectrum.listener.EventListener;

public class Spectrum extends PluginBase {
    protected APIThread apiThread = null;

    private static Spectrum instance;

    public static Spectrum get() {
        return instance;
    }

    @Override
    public void onEnable() {
        instance = this;
        if (this.getConfig().exists("api")) {
            ConfigSection section = this.getConfig().getSection("api");
            if (section.getBoolean("enabled", true)) {
                registerAPIThread(section);
            }
        }

        getServer().getPluginManager().registerEvents(new EventListener(), this);
    }

    private void registerAPIThread(ConfigSection section) {
        this.apiThread = new APIThread(
                this.getLogger(),
                section.getString("token"),
                section.getString("address"),
                section.getInt("port")
        );
        this.apiThread.start();
    }
}
