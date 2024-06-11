## Prototype

The architecture follows master-worker style, like many state-of-the-art distributed file storage such as HDFS and Ceph. Four major components are client, coordinator, proxy and datanode.

### Environment Configuration

- GCC and CMake version

  - gcc 9.4.0

  - cmake 3.22.0

- Required packages

  * grpc v1.50

  * asio 1.24.0

  * jerasure

- we call them third_party libraries, and the source codes are provided in the `third_party/` directory.

- Before installing these packages, you should install the dependencies of grpc.

  - ```
    sudo apt install -y build-essential autoconf libtool pkg-config
    ```

- Run the following command to install these packages

  - ```
    sh install_third_party.sh
    ```

### Compile and Run

- Compile

```
cd project
sh compile.sh
```

- Run

```
sh run_proxy_datanode.sh
cd project/cmake/build
./run_coordinator
./run_client ...
```

- The parameter meaning of `run_client`

```
./run_client test_type partial_decoding encode_type singlestripe_placement_type global_parity_num storage_overhead block_size(KB) stripe_num is_range tracefilename
# e.g.
./run_client false false U_LRC Opt 3 1.3 1024 4 false tracefile.txt
```

#### Tips. 

- `test_type`
  - `fasle` - all stripes are encoded with the same coding parameters
  - `true` - each stripe has its own coding parameters

- `encode_type` denotes the encoding type of a single stripe, such as  `Azu_LRC(Azure LRC)`,`U_LRC(Uniform LRC) ` or `NU_LRC(Non-uniform LRC)` (supported)
- `singlestripe_placement_type` - data placement type of a single stripe
  - `Opt` - place every `g+1` blocks in a single cluster for each local group, subjecting to single-cluster fault tolerance
  - `Flat` - place every block in a distinct cluster, subjecting to multi-cluster fault tolerance

- `global_parity_num` denotes the number of global parities in a stripe
- `storage_overhead ` denotes the storage overhead of stripes in the storage system, can be calculated as `total_block_num_in_a_stripe/data_block_num_in_a_stripe` 
- `block_size` denotes the size of each block in a stripe, with the unit of `KiB`
- `stripe_num` denotes the number of stripes in the storage system for test
- `is_range` denotes if the stripes for test have storage overheads in a specified range
- `tracefilename` - the filename of tracefile

### Other

#### Directory

- directory `project/` is the system implementation.
- create directory `data/` to store the original test data object for the client to upload.
- create directory `storage/` to store the data blocks for data nodes.
- create directory `run_cluster_sh/` to store the running shell for each cluster.
- create directory `tracefile/` to store the tracefile for test.

To create necessary directories:

```
sh create_dirs.sh
```


#### Tools

- use `small_tools/generator_file.py` to generate files with random string of specified length.
- use `small_tools/generator_sh.py` to generate configuration file and running shell for proxy and data node.
- use `small_tools/generator_tracefile.py` to generate testing tracefiles with files-information to be organized in stripes.

  - The content of tracefile is organized in a form like

    ```
    (object1_size,object1_accessrate),(object2_size,object2_accessrate),... 	# for stripe 1
    (object1_size,object1_accessrate),(object2_size,object2_accessrate),... 	# for stripe 2
    ...
    ```

    - a line per stripe
    - different stripes may consist of different number of objects, for `test_type == true `
    - only one line, for `test_type == false `
    
  - If is for stripes with different storage overheads, the form is
  
    ```
    storage_overhead,global_parity_num,(object1_size,object1_accessrate),(object2_size,object2_accessrate),... 	# for stripe 1
    storage_overhead,global_parity_num,(object1_size,object1_accessrate),(object2_size,object2_accessrate),... 	# for stripe 2
    ...
    ```
  
    - a line per stripe

