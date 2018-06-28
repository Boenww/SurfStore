package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;
import surfstore.SurfStoreBasic.WriteResult.Result;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;
    private static ManagedChannel blockChannel;
    private static BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    public MetadataStore(ConfigReader config) {
        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
	}

	private void start(int serverID, int port, int numThreads, ConfigReader config) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(serverID, config))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(c_args.getInt("number"), config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"), config);
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        protected Map<String, FileInfo> fileMap;
        protected boolean isLeader;
        protected boolean isCrashed;

        public MetadataStoreImpl(int serverID, ConfigReader config) {
            super();
            this.fileMap = new HashMap<String, FileInfo>();
            this.isCrashed = false;
            this.isLeader = (serverID == config.getLeaderNum());

//            this.Log = new ArrayList<>();
//            this.lastApplied = -1;
//            if (this.isLeader) {
//                for (int i = 1; i <= config.getNumMetadataServers(); i++) {
//                    ManagedChannel metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(i))
//                            .usePlaintext(true).build();
//                    MetadataStoreGrpc.MetadataStoreBlockingStub ms = MetadataStoreGrpc.newBlockingStub(metadataChannel);
//                    if (i != config.getLeaderNum()) {
//                        metadataChannels.add(metadataChannel);
//                        metadataStubs.add(ms);
//                    }
//                }
//            }
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            String fileName = request.getFilename();
            FileInfo.Builder builder = FileInfo.newBuilder();

            if (isCrashed) {
                builder.setFilename(fileName);
                builder.setVersion(fileMap.get(fileName).getVersion());
            } else {
                logger.info("Reading file with name " + fileName);
                if (fileMap.containsKey(fileName)) {
                    builder.setFilename(fileName);
                    builder.setVersion(fileMap.get(fileName).getVersion());
                    builder.addAllBlocklist(fileMap.get(fileName).getBlocklistList());
                    //addAllBlocklist(java.lang.Iterable<java.lang.String> values)
                    //addBlocklist(fileMap.get(fileName).getBlocklistList());
                } else {
                    builder.setVersion(0);
                }
            }

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            WriteResult.Builder builder = WriteResult.newBuilder();

            if (isCrashed || !isLeader) {
                builder.setResult(Result.NOT_LEADER);
            } else {
                String fileName = request.getFilename();
                logger.info("Modifying file with name " + fileName);
                int requestVersion = request.getVersion();

                if (!fileMap.containsKey(fileName) || requestVersion == 0) {
                    //create
                    if (requestVersion == 1) {
                        for (int i = 0; i < request.getBlocklistCount(); i++) {
                            String blockHash = request.getBlocklist(i);
                            Block block = Block.newBuilder().setHash(blockHash).build();
                            if (!blockStub.hasBlock(block).getAnswer()) {
                                builder.addMissingBlocks(blockHash);
                            }
                        }
                        if (builder.getMissingBlocksCount() > 0) {
                            //miss
                            builder.setCurrentVersion(0);
                            builder.setResult(Result.MISSING_BLOCKS);
                        } else {
                            //no miss
                            fileMap.put(fileName, request);
                            builder.setCurrentVersion(requestVersion);
                            builder.setResult(Result.OK);
                        }
                    } else {
                        //wrong version
                        builder.setCurrentVersion(0);
                        builder.setResult(Result.OLD_VERSION);
                    }

                } else {
                    //update
                    int currentVersion = fileMap.get(fileName).getVersion();

                    if (requestVersion == currentVersion + 1) {
                        for (int i = 0; i < request.getBlocklistCount(); i++) {
                            String blockHash = request.getBlocklist(i);
                            Block block = Block.newBuilder().setHash(blockHash).build();
                            if (!blockStub.hasBlock(block).getAnswer()) {
                                builder.addMissingBlocks(blockHash);
                            }
                        }
                        if (builder.getMissingBlocksCount() > 0) {
                            //miss
                            builder.setCurrentVersion(currentVersion);
                            builder.setResult(Result.MISSING_BLOCKS);
                        } else {
                            //no miss
                            fileMap.put(fileName, request);
                            builder.setCurrentVersion(requestVersion);
                            builder.setResult(Result.OK);
                        }
                    } else {
                        //wrong version
                        builder.setCurrentVersion(currentVersion);
                        builder.setResult(Result.OLD_VERSION);
                    }
                }
            }
            WriteResult response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        }

        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            WriteResult.Builder builder = WriteResult.newBuilder();

            if (isCrashed || !isLeader) {
                builder.setResult(Result.NOT_LEADER);
            } else {
                String fileName = request.getFilename();
                logger.info("Deleting file with name " + fileName);

                if (fileMap.containsKey(fileName)) {
                    int currentVersion = fileMap.get(fileName).getVersion();
                    int requestVersion = request.getVersion();

                    if (requestVersion == currentVersion + 1) {
                        FileInfo.Builder newFileInfob = FileInfo.newBuilder();
                        newFileInfob.setFilename(fileName);
                        newFileInfob.setVersion(requestVersion);
                        newFileInfob.addAllBlocklist(Arrays.asList("0"));

                        FileInfo newFileInfo = newFileInfob.build();
                        fileMap.put(fileName, newFileInfo);

                        builder.setCurrentVersion(requestVersion);
                        //builder.addMissingBlocks(Arrays.asList('0'));
                        builder.setResult(Result.OK);
                    } else {
                        //wrong version
                        builder.setCurrentVersion(currentVersion);
                        builder.setResult(Result.OLD_VERSION);
                    }

                } else {
                    //not exist
                    builder.setCurrentVersion(0);
                    builder.setResult(Result.OLD_VERSION);
                }
            }
            WriteResult response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            logger.info("Testing if it is the leader...");

            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isLeader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            logger.info("Crashing server...");
            if (!isLeader) {
                isCrashed = true;
            } else {
                throw new RuntimeException("Crash leader failed!");
            }

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            logger.info("Restoring server...");
            isCrashed = false;

            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            logger.info("Testing if it is crashed ");

            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(isCrashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            logger.info("Getting version of the file with name " + request.getFilename());
            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setFilename(request.getFilename());

            if (fileMap.containsKey(request.getFilename())) {
                builder.setVersion(fileMap.get(request.getFilename()).getVersion());
                builder.addAllBlocklist(fileMap.get(request.getFilename()).getBlocklistList());//
            } else {
                builder.setVersion(0);
            }

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
}