package com.github.emitskevich.core.server;

import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.config.FileConfigProvider;
import com.google.devtools.common.options.OptionsParser;
import java.util.function.BiConsumer;
import com.github.emitskevich.core.config.EnvConfigProvider;

public class LauncherBase {

  public static void main(String[] args, BiConsumer<ServerContext, AppConfig> registerInstances) {
    OptionsParser parser = OptionsParser.newOptionsParser(ServerOptions.class);
    parser.parseAndExitUponError(args);
    ServerOptions options = parser.getOptions(ServerOptions.class);

    ServerContext context = new ServerContext();

    FileConfigProvider fileConfigProvider = new FileConfigProvider(options.config);
    AppConfig appConfig = new AppConfig(new EnvConfigProvider(), fileConfigProvider);
    registerInstances.accept(context, appConfig);
    Application application = new Application(context);

    application.registerShutdownHook();
    application.initialize();
    application.start();
  }
}
