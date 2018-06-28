package surfstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;
import surfstore.SurfStoreBasic.WriteResult.Result;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final List<ManagedChannel> metadataChannels;
    private final List<MetadataStoreGrpc.MetadataStoreBlockingStub> metadataStubs;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;

    public Client(ConfigReader config) {
        this.metadataChannels = new ArrayList<>();
        this.metadataStubs = new ArrayList<>();

        for (int i = 1; i <= config.getNumMetadataServers(); i++) {
            ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i))
                    .usePlaintext(true).build();
            MetadataStoreGrpc.MetadataStoreBlockingStub ms = MetadataStoreGrpc.newBlockingStub(metadataChannel);

            if (i == config.getLeaderNum()) {
                this.metadataStub = ms;
            }
            this.metadataChannels.add(metadataChannel);
            this.metadataStubs.add(ms);
        }

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
    }

    public void shutdown() throws InterruptedException {
        for (ManagedChannel metadataChannel: this.metadataChannels) {
            metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private void ensure(boolean b) {
        if (b == false) {
            throw new RuntimeException("Assertion failed!");
        }
    }

    private static Block stringToBlock(String s) {
        Block.Builder builder = Block.newBuilder();
        try {
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }

        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(1);
        }
        byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
        builder.setHash(Base64.getEncoder().encodeToString(hash));

        return builder.build();
    }

    public void testBlockStore() {
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        Block b1 = stringToBlock("block_01");
        Block b2 = stringToBlock("block_02");

        ensure(blockStub.hasBlock(b1).getAnswer() == false);
        ensure(blockStub.hasBlock(b2).getAnswer() == false);

        blockStub.storeBlock(b1);
        ensure(blockStub.hasBlock(b1).getAnswer() == true);

        blockStub.storeBlock(b2);
        ensure(blockStub.hasBlock(b2).getAnswer() == true);

        Block b1prime = blockStub.getBlock(b1);
        ensure(b1prime.getHash().equals(b1.getHash()));
        ensure(b1.getData().equals(b1.getData()));

        logger.info("We passed all the tests... yay!");
    }

    private void upload(String pathToFile) {
        Map<String, byte[]> blockMap = new HashMap<>();
        try {
            File file = new File(pathToFile);
            String fileName = file.getName();
            FileInputStream fileInputStream = new FileInputStream(file);

            byte[] block = new byte[4096];
            int readAmount = 0;
            FileInfo.Builder builder = FileInfo.newBuilder().setFilename(fileName);// for modify request
            FileInfo readResponse = metadataStub.readFile(FileInfo.newBuilder().setFilename(file.getName()).build());

            while((readAmount = fileInputStream.read(block)) >= 0) {
                if(readAmount> 0) {
                    byte[] currentBlock = Arrays.copyOfRange(block, 0, readAmount);
                    MessageDigest digest = null;
                    try {
                        digest = MessageDigest.getInstance("SHA-256");
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                    String currentHash = Base64.getEncoder().encodeToString(digest.digest(currentBlock));
                    blockMap.put(currentHash, currentBlock);
                    builder.addBlocklist(currentHash);
                }
            }

            boolean flg = true;
            while (flg) {
                builder.setVersion(readResponse.getVersion() + 1);
                FileInfo request = builder.build();
                WriteResult modifyResponse = metadataStub.modifyFile(request);
                switch (modifyResponse.getResult()) {
                    case OK:
                        System.out.println("OK");
                        logger.info("File with name " + fileName + " uploaded.");
                        flg = false;
                        break;
                    case OLD_VERSION:
                        logger.info("Upload Error: Requires version >= " + String.valueOf((modifyResponse.getCurrentVersion()) + 1));
                        readResponse = metadataStub.readFile(request);
                        break;
                    case MISSING_BLOCKS:
                        for (String s: modifyResponse.getMissingBlocksList()) {
                            Block missBlock = Block.newBuilder().setHash(s).build();
                            if (!blockStub.hasBlock(missBlock).getAnswer()) {
                                blockStub.storeBlock(Block.newBuilder().setHash(s).setData(ByteString.copyFrom(blockMap.get(s))).build());
                            }
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unexpected WriteResult.Result value: " + modifyResponse.getResult().toString());
                }
            }
            fileInputStream.close();

        } catch (FileNotFoundException e) {
            logger.warning("Upload file with path " + pathToFile + " failed!");
            System.out.println("Not Found");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void download(String fileName, String pathToStore) {
        FileInfo request = FileInfo.newBuilder().setFilename(fileName).build();
        FileInfo readResponse = metadataStub.readFile(request);
        if (readResponse.getVersion() == 0 || readResponse.getBlocklistList().equals(Arrays.asList('0'))) {
            System.out.println("Not Found");
            logger.warning("Download file with name " + fileName + " failed!");
        }

        //check blocks locally
        Map<String, byte[]> localBlockMap = new HashMap<>();
        File dir = new File(pathToStore);
        File[] files = dir.listFiles();
        for (File file: files) {
            try {
                if (file.isFile()) {
                    FileInputStream fileInputStream = new FileInputStream(file);
                    byte[] block = new byte[4096];
                    int readAmount = 0;

                    while((readAmount = fileInputStream.read(block)) >= 0) {
                        if(readAmount> 0) {
                            byte[] currentBlock = Arrays.copyOfRange(block, 0, readAmount);
                            MessageDigest digest = null;
                            try {
                                digest = MessageDigest.getInstance("SHA-256");
                            } catch (NoSuchAlgorithmException e) {
                                e.printStackTrace();
                                System.exit(1);
                            }
                            String currentHash = Base64.getEncoder().encodeToString(digest.digest(currentBlock));
                            localBlockMap.put(currentHash, currentBlock);
                        }
                    }
                    fileInputStream.close();
                }
            } catch (FileNotFoundException e) {
                logger.warning("Check blocks locally failed!");
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        //download
        try {
            File file = new File(pathToStore, fileName);
            FileOutputStream fileOutputStream = new FileOutputStream(file);
            for (String s: readResponse.getBlocklistList()) {
                if (localBlockMap.containsKey(s)) {
                    fileOutputStream.write(localBlockMap.get(s));
                } else {
                    Block.Builder blockb = Block.newBuilder().setHash(s);
                    fileOutputStream.write(blockStub.getBlock(blockb.build()).getData().toByteArray());
                }
            }
            fileOutputStream.close();
            System.out.println("OK");
            logger.info("File with name " + fileName + " downloaded.");
        } catch (FileNotFoundException e) {
            System.out.println("Not Found");
            logger.warning("Download file with name " + fileName + " failed!");
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void delete(String fileName) {
        FileInfo.Builder builder = FileInfo.newBuilder().setFilename(fileName);
        FileInfo readResponse = metadataStub.readFile(FileInfo.newBuilder().setFilename(fileName).build());
        if (readResponse.getVersion() == 0 || readResponse.getBlocklistList().equals(Arrays.asList('0'))) {
            System.out.println("Not Found");
            logger.warning("Delete file with name " + fileName + " failed!");
            return;
        }

        boolean flg = true;
        while (flg) {
            builder.setVersion(readResponse.getVersion() + 1);
            WriteResult deleteResponse = metadataStub.deleteFile(builder.build());
            switch (deleteResponse.getResult()) {
                case OK:
                    System.out.println("OK");
                    logger.info("File with name " + fileName + " deleted.");
                    flg = false;
                    break;
                case OLD_VERSION:
                    logger.info("Delete Error: Requires version >= " + String.valueOf(readResponse.getVersion() + 1));
                    readResponse = metadataStub.readFile(builder.build());
                    break;
                default:
                    throw new IllegalStateException("Unexpected WriteResult.Result value: " + deleteResponse.getResult().toString());
            }
        }
    }

    private void getVersion(String fileName) {
        FileInfo request = FileInfo.newBuilder().setFilename(fileName).build();
        String versions = "";

        for (int i = 0; i < this.metadataStubs.size(); i++) {
            FileInfo response = metadataStub.readFile(request);
            versions += String.valueOf(response.getVersion());
            if (i != this.metadataStubs.size() - 1) {
                versions += " ";
            }
        }

        System.out.println(versions);
        logger.info("Get version of the file with name " + fileName + ": " + versions);
    }

    private void crash(String MetadataServer_index) {
        int index = Integer.parseInt(MetadataServer_index);

        Empty request = Empty.newBuilder().build();
        this.metadataStubs.get(index - 1).crash(request);
    }

    private void restore(String MetadataServer_index) {
        int index = Integer.parseInt(MetadataServer_index);

        Empty request = Empty.newBuilder().build();
        this.metadataStubs.get(index - 1).restore(request);
    }

	private void go(Namespace c_args) {
        metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");

        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        String fileName = c_args.getString("file_name / MetadataServerID");

        switch (c_args.getString("command")) {
            case "upload":
                upload(fileName);
                break;
            case "download":
                download(fileName, c_args.getString("download_path"));
                break;
            case "delete":
                delete(fileName);
                break;
            case "getversion":
                getVersion(fileName);
                break;
            case "crash":
                crash(fileName);
                break;
            case "restore":
                restore(fileName);
            default:
                break;
        }
	}

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("command").type(String.class)
                .choices("upload", "download", "delete", "getversion", "crash", "restore")
                .help("Command to execute");
        parser.addArgument("file_name / MetadataServerID").type(String.class)
                .help("File name or path to file when upload / ServerID to crash or restore");
        parser.addArgument("download_path").type(String.class)
                .nargs("?")
                .help("Path to where to store it");

        
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);
        
        try {
        	client.go(c_args);
        } finally {
            client.shutdown();
        }
    }

}
