# Scad: Serverless Computing with Aggregation and Disaggregation

## Project Structure

- `openwhisk`: Modified openwhisk system
- `runtime`: Container runtime support for disaggregation
    - `lib`: Runtime library (`libd`) for element management and communication
    - `openwhisk-runtime-go`: Modified runtime for openwhisk container
    - `openwhisk-runtime-python`: Modified runtime and language binding for python
    - `openwhisk-runtime-nodejs`: [decrapted] Modified runtime and language binding for nodejs
    - `scripts`: Scripts for testing with libd, containers and openwhisk
- `front-end`: [work-in-progress] Tools for generating Scad DAG from full program
- `wskish`: Command line tool for communication with modified runtime

## Scad Programming Model and Workflow

Scad programs are written in a DAG whose vertexes are elements and edges are relationships. Elements in DAG have two kinds of relationships, corunning and dependent. Communication can only happen between corunning elements.

### Programming Elements with Scad APIs

The input of Scad is a DAG of elements. Currently Scad supports compute elements written in python and memory elements.
The DAG is defined as a separate directory and elements are defined as files with format "<element-name>.o.<ext-type>". Elements files are begin with meta data defining its relationships with other elements. Currently python files `.o,py` are used to define compute elements and `.o.yaml` to define memory elements. A simple example for Scad Program and Elements can be find in `runtime/test/src/corunning-2-node`.

Compute elements can use Scad APIs to access memory elements. To access memory elements, first call `get_transport('<mem-element-name>')` to get instance of memory element, then use `trans.buf()` to create access buffer. Transfer calls `trans.read()` and `trans.write()` can be used to transfer data between buffer and memory elements. The full list of APIs can be found in `runtime/lib/ext/python/disagg.pyx`. A more complex example (Logistic regression) can be found in `runtime/test/src/logistic_regression/splitted_program/first`.

On top of basic APIs, Scad provides high-level APIs. High-level APIs provide interfaces to accessing remote data structures but hiding the details of remote memory.
- Data Frame APIs, see `runtime/lib/ext/npjoin`
- Numpy Array APIs (with buffering layer), see `runtime/openwhisk-runtime-python/disaggrt/disaggrt`

### Generating JSON and submitting to the system.

Scad provides `wskgen` commandline tool to generate JSON DAG file from program directory and `wskish` tool to submit to modified OpenWhisk runtime.

However, testing Scad programs do not always need to bring to the whole Scad system as we will show in next section.

## Testing Scad Programs

### Testing Scad Elements

Scad providing a set of tools for testing elements without Openwhisk and Docker environment. Memory elements are emulated by `memory_server` and compute elements are supported with local python runtime.

#### Environment Setup

Linux environment is required to build element testing environment. On Ubuntu (including Ubuntu for WSL), run

```
sudo apt install libnanomsg-dev libcurl4-openssl-dev libglib2.0-dev libibverbs-dev
```

Python 3.6+ is required to build Scad python runtime and scripts. Install Scad libraries in a  clean virtual environment is strongly recommended. Required packages can be installed with:

```
pip install cython requests numpy pyyaml
```

Scad Local environment can be built with:
```
make -C runtime/lib ext_python
```

#### Verifying the environment.

To enable the environment, run `source runtime/scripts/activation`.
To verify the python language library, run `python runtime/lib/tests/scripts/runtime_example_local.py`. The expected results should be:

```
[libd.c:60] Action 0000 initd with argc 0

[durl.c:46] parsing durl client1;rdma_local;
[libd.c:137] Init Transport with name client1
[transports/rdma_local.c:18] init parameter missing size
[libd_transport.c:35] state transaction ABORT (1, 0)
[libd.c:142] init fail with rv -1
[durl.c:34] parsing config size 65536
[transports/rdma_local.c:19] init local memory with size 65536
[libd_transport.c:39] state transaction SUCCESS (0, 1)
[libd_transport.c:55] state transaction SUCCESS (1, 3)
[interfaces/libd_trdma.c:27] state transaction SUCCESS (4, 3)
[interfaces/libd_trdma.c:43] state transaction SUCCESS (4, 3)
API test int = 12345, fetch = 12345 True
[transports/rdma_local.c:31] calling terminate with 65536 size
```
To verify the simulated memory element, launch one simulated memory with `runtime/lib/memory_server` in background, then run `python runtime/lib/tests/scripts/runtime_example_local.py`. The expected result should be:

```
$ memory_server
[libd.c:60] Action 00000000 initd with argc 0

[durl.c:46] parsing durl server;rdma_tcp_server;url,tcp://*:2333;size,1073741824;
[durl.c:34] parsing config url tcp://*:2333
[durl.c:34] parsing config size 1073741824
[libd.c:137] Init Transport with name server
[transports/rdma_tcp_server_nanomsg.c:15] server init
[libd_transport.c:39] state transaction SUCCESS (0, 1)
[transports/rdma_tcp_server_nanomsg.c:40] server connect
[libd_transport.c:55] state transaction SUCCESS (1, 3)
[transports/rdma_tcp_server_nanomsg.c:62] start servering at tcp://*:2333
[transports/rdma_tcp_server_nanomsg.c:70] get a message of size 28
[transports/rdma_tcp_server_nanomsg.c:70] get a message of size 24

$ python runtime/lib/tests/scripts/runtime_example_local.py
[libd.c:60] Action 0000 initd with argc 0

[durl.c:46] parsing durl client1;rdma_tcp;
[libd.c:137] Init Transport with name client1
[transports/rdma_tcp_nanomsg.c:58] start setup for client1
[transports/rdma_tcp_nanomsg.c:60] init parameter missing url
[libd_transport.c:35] state transaction ABORT (1, 0)
[libd.c:142] init fail with rv -1
[durl.c:34] parsing config url tcp://localhost:2333
url -> tcp://localhost:2333
[transports/rdma_tcp_nanomsg.c:58] start setup for client1
[transports/rdma_tcp_nanomsg.c:67] setup for client1: size 994305392, url tcp://localhost:2333
[libd_transport.c:39] state transaction SUCCESS (0, 1)
[transports/rdma_tcp_nanomsg.c:84] Setup connection rv 1
[libd_transport.c:55] state transaction SUCCESS (1, 3)
[interfaces/libd_trdma.c:27] state transaction SUCCESS (4, 3)
[interfaces/libd_trdma.c:43] state transaction SUCCESS (4, 3)
API test int = 12345, fetch = 12345 True
```

#### Launching Scad Programs

Script `rundisagg` is provided to test Scads locally. We manually launch memory elements with `memory_server <port-number>` and launch compute elements with `rundisagg <compute-element> -m '<memory-name>:<memory-port>'. We can simulate the whole process by following the dependency relationships.

A simple example can be found in `runtime/test/src/corunning-2-node`. To launch this program, we run these commands in a row:
```
$ memory_server 2333 # run this in background
$ rundisagg compute1.o.py -m 'mem1:2333'
$ rundisagg compute2.o.py -m 'mem1:2333'
```

A more complex examples can be found in `runtime/test/src/run_tpcds.sh`.

### Testing Scad Containers

Scad programs can be tested with Docker container environment. Container level testing should be done ONLY IF you are changing the container interface.

Script `wskruntime` can be used to test Scad at container level. run `cd runtime/openwhisk-runtime-python/core/python3ActionLoop && make` to build container`. Run `wskruntime -h` to get detials.

### Testing Scad with Scheduling System and RDMA Communication

Container level testing should be done ONLY IF you are looking for performance numbers with RDMA networking.






