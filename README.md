# SurfStore

SurfStore is a networked file storage application that supports four basic commands:
- Create a file
- Read the contents of a file
- Change the contents of a file
- Delete a file

Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. 

The SurfStore service is composed of the following two sub-services:
- BlockStore: The content of each file in SurfStore is divided up into chunks, or blocks, each of which has a unique identifier. The BlockStore service stores these blocks, and when given an identifier, retrieves and returns the appropriate block.

- MetadataStore: The MetadataStore service holds the mapping of filenames/paths to blocks.

It consists of three types of processes: client processes, a BlockStore process, and one or more Metadata processes. Note that one (and only one) of the Metadata processes is specially designated as the “leader”, meaning that all client requests should go through that server and that server only.

### Client
- create, modify, read, and delete files
- APIs: upload(pathToFile), download(fileName, pathToStore), delete(fileName), getversion(fileName)

### BlockStore
- key-value in memory data store
- APIs: StoreBlock(hash, bytes), GetBlock(hash), HasBlock(hash)

### MetadataStore
- maintains the mapping of filenames to hashlists that stored in memory 
- APIs: (version, hashList) = ReadFile(fileName), ModifyFile(fileName, version, hashList), DeleteFile(fileName, version), IsLeader(Distributed: Crash, Restore, GetVersion)
