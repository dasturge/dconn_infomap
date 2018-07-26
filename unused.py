from helpers import *


def grouper(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, n))
        if not chunk:
            return
        yield chunk


def nii2bytes(nii):
    """
    reads the values from a dconn into raw bytes.
    :param nii: filename of dconn
    :param f: output bytes file descriptor
    :return:
    """
    for arr in iterate_dconn(nii, 1000):
        floats = array.array('f', arr)
        yield floats


def numericsfromfile(f):
    while True:
        a = array.array('f')
        a.fromfile(f.read(a.itemsize * 1000))
        if not a:
            break
        for x in a:
            yield x


def niiquantile_fileio(nii, quantile):
    """
    sorts values from a dconn without loading much into memory.  Typical dconns
    are 32GB ~ 8 billion floating point numbers
    :param nii: filename of dconn
    :return:
    """
    t1 = time.perf_counter()
    niiterator = iterate_dconn(nii, 100)
    iters = []
    for a in niiterator:
        f = tempfile.TemporaryFile()
        a.sort()
        a.tofile(f)
        f.seek(0)
        iters.append(numericsfromfile(f))
    t = time.perf_counter() - t1
    print('read and sorted nifti sections in %f seconds' % t)
    print('proceeding with merge sort')

    t1 = time.perf_counter()
    a = array.array('f')
    with open('tmpnii', 'wb') as fd:
        for x in heapq.merge(*iters):
            a.append(x)
            if len(a) >= 1000:
                a.tofile(fd)
                del a[:]
        if a:
            a.tofile(fd)
    t = time.perf_counter() - t1
    print('merge sort successful. Took %f seconds' % t)
    size = os.stat('tmpnii').st_size
    pos = size * quantile
    assert pos % array.array('f').itemsize == 0
    with open('tmpnii', 'rb') as fd:
        fd.seek(pos)
        typing = array.array('f')
        bits = fd.read(typing.itemsize)
        out = typing.frombytes(bits)
    os.remove('tmpnii')

    return out