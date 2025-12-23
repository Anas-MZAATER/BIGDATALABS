package net.anas;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Usage: ReadHDFSFile <full-path-to-file>");
            System.exit(1);
        }

        String filePathStr = args[0];  // Chemin complet du fichier HDFS

        Configuration conf = new Configuration();
        FileSystem fs = null;
        BufferedReader br = null;

        try {
            fs = FileSystem.get(conf);
            Path path = new Path(filePathStr);

            // Vérifie si le fichier existe
            if(!fs.exists(path)) {
                System.out.println("File does not exist: " + filePathStr);
                System.exit(1);
            }

            // Ouvre le fichier pour lecture
            br = new BufferedReader(new InputStreamReader(fs.open(path)));

            String line;
            System.out.println("Contenu du fichier HDFS :");
            while((line = br.readLine()) != null) {
                System.out.println(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(br != null) br.close();
                if(fs != null) fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
