function main( params,action) {
    let t = action.get_transport('client','rdma');
    let buf = t.alloc(64);

    buf.write('Hello RDMA', 'utf-8');
    t.write(0, 64, buf);
    t.read(0, 64, buf);

    let ret = buf.toString('utf-8');

    return {payload: ret};
}
