Scad Python Binding
---

This library provides python level binding for Scad C Runtime (libd). It exposes management and data path operations for Scad Python Programs. The functions are defined in cython file `disagg.pyx`.

## Usage

All Scad Python programs are injected with an instance of `class LibdAction` object as the second parameter of its main function. User should relies instance functions provided by this object to call Scad functions.

## APIs

### Action APIs

We assume the parameter is named as `action`. Raise Exception when name or type is not found.

Get transport by type and name. Note that the returned transport may not be usable. Any call on that object will be blocked until it is properly initialized by runtime.

```python
# name: string Name of transport. Should be the same name of connected object name.
# ttype: string Type of Transport. Valid values: `rdma` and `rdma_server`.
# [return] trans: LibdTransport Requested Transport Object

trans = action.get_transport(name, ttype)
```

Internal APIs are provided for Scad management functions. User should not call those functions.

```python
action.terminate()
action.add_transport()
action.config_transport()
```

### Runtime APIs

Runtime APIs provide interfaces to Scad managing and scheduling system. Those APIs can be accessed by object `action.runtime`. Note that **these APIs only work within OpenWhisk Runtime**.

Invoke new element and build transport to new element. One common use case is to scale up/down compute/memory elements.

```python
# template: string Template of the new element. Features an existing element's name
# trans: string New transport name for new element
# [optional] props Props need to be changed based on template element

action.runtime.invoke(template, trans, [props])
```

Release connections to another elements. After this function is called, all existing transports to target elements are destroyed and creation of new transports will cause undefined behavior.

```python
# name: string Name of the element to be released

action.runtime.release(name)
```


### LibdRDMATransport API

Transport is a link between local compute element and another remote element. RDMATransport allows compute object to access memory objects through Remote Direct Memory Access interface. To use this interface, user need to register a RDMA-enabled buffer and then transfer data between local buffer and remote memory object. The  instance object is returned by `action.get_transport(name, 'rdma')`

Register RDMA Buffer. The function has no return value.

```python
# size: int                           Size of the buffer. 
# [optional] trans: LibdRDMATransport Other transport if you want to share buffer with existing transport.

trans.reg(size, [trans])
```

Synchronized Read. Transfer data from remote memory object to local buffer. Raise `MemoryError` if no buffer is registered. Block if transport is not initialized.

```python
# size: int   Size of the transfer
# addr: int   Target Address in remote memory element
# offset: int Offset Address in local buffer

trans.read(size, addr, offset)
```

Synchronized Write. Transfer data from local buffer to remote object. Raise `MemoryError` if no buffer is registered. Block if transport is not initialized.

```python
# size: int   Size of the transfer
# addr: int   Target Address in remote memory element
# offset: int Offset Address in local buffer
trans.write(size, addr, offset)
```

