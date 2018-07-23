import array
import heapq
import os
import subprocess
import tempfile

import h5py
import nibabel
import numpy as np


def nii2bytes(nii):
    """
    reads the values from a dconn into raw bytes.
    :param nii: filename of dconn
    :param f: output bytes file descriptor
    :return:
    """
    for arr in iterate_dconn(nii):
        floats = array.array('f', arr)
        yield floats


def numericsfromfile(f):
    while True:
        a = array.array('f')
        a.fromfile(f.read(a.itemsize * 8 * 1000))
        if not a:
            break
        for x in a:
            yield x


def niiquantile(nii, quantile):
    """
    sorts values from a dconn without loading into memory.  Typical dconns
    are 32GB ~ 1 billion floating point numbers
    :param nii: filename of dconn
    :return:
    """
    niiterator = nii2bytes(nii)
    iters = []
    for a in niiterator:
        f = tempfile.TemporaryFile()
        array.array('f', sorted(a)).tofile(f)
        f.seek(0)
        iters.append(numericsfromfile(f))

    a = array.array('f')
    with open('tmpnii', 'wb') as fd:
        for x in heapq.merge(*iters):
            a.append(x)
            if len(a) >= 1000:
                a.tofile(fd)
                del a[:]
        if a:
            a.tofile(fd)
    size = os.stat('tmpnii').st_size * 8  # to bits
    quant = size * (1 - quantile) // (array.array('d').itemsize * 8)
    with open('tmpnii', 'rb') as fd:
        fd.seek(quant)
        typing = array.array('d')
        bits = fd.read(typing.itemsize * 8)
        out = typing.frombytes(bits)
    os.remove('tmpnii')

    return out


def iterate_dconn(filename):
    nii = nibabel.load(filename)
    for idx in range(nii.shape[-1]):
        yield nii.dataobj[..., idx]


def get_shape(filename):
    nii = nibabel.load(filename)
    return nii.shape


def create_pajek(rmat, dmat, dist_thresh, tie_density, output):
    shape = get_shape(rmat)
    dmat = [x for x in h5py.File(dmat).values()][0]
    with open(output, 'w') as out:
        # write vertices to pajek file
        out.write('*Vertices %s' % shape[-1] + '\n')
        gen = ('%s "%s"\n' % (i, i) for i in range(1, shape[-1] + 1))
        out.writelines(gen)

        # compute edge density threshold on disk for data too big for mem
        thresh = niiquantile(rmat, tie_density)

        # iterate through reading rows from disk to conserve system memory
        for idx, array in enumerate(iterate_dconn(rmat)):

            # ensure diagonal set to zero
            if dist_thresh <= 0:
                array[idx] = 0

            # threshold distances
            remove_dist = np.argwhere(dmat[..., idx] < dist_thresh)
            array[remove_dist] = 0

            # threshold percentages
            remove_thresh = np.argwhere(array < thresh)
            array[remove_thresh] = 0

            # write values to temporary file.

        # write total number of edges then append temporary file.


if __name__ == '__main__':
    import sys
    filename = sys.argv[1]
    dmat = sys.argv[2]
    create_pajek(filename, dmat, 15, .05, 'test.pajek')
