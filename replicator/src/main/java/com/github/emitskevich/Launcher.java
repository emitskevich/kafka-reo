package com.github.emitskevich;

import com.github.emitskevich.core.config.EnvConfigProvider;
import com.github.emitskevich.core.config.FileConfigProvider;
import com.github.emitskevich.core.server.Application;
import com.github.emitskevich.core.server.ServerOptions;
import com.github.emitskevich.kafka.KafkaClients;
import com.github.emitskevich.core.config.AppConfig;
import com.github.emitskevich.core.server.ServerContext;
import com.google.devtools.common.options.OptionsParser;
import java.util.function.BiConsumer;

public class Launcher {

  public static void main(String[] args) {
    OptionsParser parser = OptionsParser.newOptionsParser(ServerOptions.class);
    parser.parseAndExitUponError(args);
    ServerOptions options = parser.getOptions(ServerOptions.class);

    ServerContext context = new ServerContext();

    FileConfigProvider fileConfigProvider = new FileConfigProvider(options.config);
    AppConfig appConfig = new AppConfig(new EnvConfigProvider(), fileConfigProvider);
    ((BiConsumer<ServerContext, AppConfig>) Launcher::registerInstances).accept(context, appConfig);
    Application application = new Application(context);

    application.registerShutdownHook();
    application.initialize();
    application.start();
  }

  private static void registerInstances(ServerContext context, AppConfig appConfig) {
    context.register(KafkaClients.class, new KafkaClients(appConfig));
  }
}
