# Libd Runtimes

## Running Python Runtime Local Tests

prerequests:
- linux server with `libnanomsg-dev` and `libcurl-dev` installed.
- python3 installed. cython installed (`pip3 install cython`)

First, goto `lib` and run `make ext_python memory_server`

Then, run `./memory_server <port>` to launch an memory object simulation on localhost

Finally, in the `lib` directory, run `LD_LIBRARY_PATH=. python runtime_example.py` to launch the example. To launch your own python, make sure the file `disagg.<cython-version>.so` is in current work directory and `libd.so` is in `LD_LIBRARY_PATH`.

## Co-running objects

with the runtime libs, we add a set of c libs as manager for transport layer between co-running objects. For each invocation, we create an separate object, `action`, associated with activationId, as the interface to the c libs. This action must be created before or with the `run` http call. (If we want to pre-heat the connections between containers, we may crate action before run using the rest APIs).

One actions contains multiple `transports`, one `transport` is a communication channel between a pair of co-running objects. channels are set by the config string:

```
NAME;TYPE;[KEY,VALUE;]*
```

where name is the name of the transport link, type is the implementation of the link; and KEY,VALUEs are parameters passed to the uderlaying implementation. For example, to use TCP we need to pass ip, port, etc.

This `action` object will be passed to use program as the second parameter (see `openwhisk-runtime-nodejs/tests/src/test/knative/hellordma/client_program.js` for details), 

## Run example co-running tests

`cd openwhisk-runtime-nodejs/tests/src/test/knative`

To run co-running tests, we need two container, one is server `as memory object`, another is client (as compute object.

We can call `bash setup-rdma.sh` to setup those two containers. After that, we need to change the config string in `hellordma/run.json` to the correct server contianer ip.

with the two container running, we need to first run `bash run.sh hellordma-server devnode-server` to launch the rdma server in `devnode-server` container. This call should block; After that, we can run `bash run.sh hellordma-client devnode-client` to run client program.


# Libd:
transport hold the callback for libd functions

http post fields:
```
object: objectname,
<timestamp>: ,[<transportname>, <rxBytes, <txBytes>]+
```

