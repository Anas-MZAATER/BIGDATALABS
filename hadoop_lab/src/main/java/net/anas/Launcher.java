package net.anas;

import java.util.Arrays;

public class Launcher {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage:");
            System.err.println("hadoop jar hadoop-app.jar <class> <hdfs-file> [new-name]");
            System.exit(1);
        }

        String className = args[0];

        // Tous les arguments sauf le nom de la classe
        String[] forwardedArgs = Arrays.copyOfRange(args, 1, args.length);

        Class<?> clazz = Class.forName(className);
        java.lang.reflect.Method main =
                clazz.getMethod("main", String[].class);

        main.invoke(null, (Object) forwardedArgs);
    }
}
