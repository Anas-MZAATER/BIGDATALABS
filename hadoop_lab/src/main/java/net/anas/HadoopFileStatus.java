package net.anas;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        if(args.length < 1) {
            System.out.println("Usage: HadoopFileStatus <full-path-to-file> [newname]");
            System.exit(1);
        }

        String filePathStr = args[0];               // Chemin complet du fichier source
        String newFileName = (args.length >= 2) ? args[1] : null; // Nom du fichier renommé

        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);
            Path filepath = new Path(filePathStr);

            if(!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.out.println("Argument 0 = " + args[0]);
                System.exit(1);
            }

            FileStatus status = fs.getFileStatus(filepath);

            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());

            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for(BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Renommage si un nouveau nom est fourni
            if(newFileName != null && !newFileName.isEmpty()) {
                Path parentDir = filepath.getParent();  // conserve le répertoire
                Path newFilePath = new Path(parentDir, newFileName);
                fs.rename(filepath, newFilePath);
                System.out.println("File renamed to: " + newFilePath.toString());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
