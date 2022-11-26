package com.github.emitskevich.core.server;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

public class ServerOptions extends OptionsBase {

  @Option(
      name = "config",
      help = "Path to config file.",
      category = "startup",
      defaultValue = ""
  )
  public String config;
}
