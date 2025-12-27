package net.anas.kafka;

import java.util.Arrays;

public class Launcher {

    public static void main(String[] args) throws Exception {
        // Vérification basique
        if (args.length < 1) {
            System.err.println("Usage: java -jar kafka-app.jar <full-class-name> [args...]");
            System.err.println("Exemple: java -jar kafka-app.jar net.anas.kafka.EventProducer Hello-Kafka");
            System.exit(1);
        }

        // 1er argument = nom complet de la classe (package + class)
        String className = args[0];

        // Tous les arguments suivants
        String[] forwardedArgs = Arrays.copyOfRange(args, 1, args.length);

        // Chargement dynamique de la classe
        Class<?> clazz = Class.forName(className);

        // Récupération de la méthode main(String[] args)
        java.lang.reflect.Method mainMethod = clazz.getMethod("main", String[].class);

        // Invocation de main avec les arguments
        mainMethod.invoke(null, (Object) forwardedArgs);
    }
}
