import os

import h5py
import dask.array as da
import numpy as np


def triu(mat):
    mat = da.ma.masked_array()


def threshold_matrix(dconn: da.Array, dmat, min_dist, tie_density):
    dconn = da.triu(dconn, k=1)
    dconn = da.where(dmat < min_dist, 0, dconn)
    dconn.to_hdf5(os.path.join(hd_path, 'tmp0.hdf5'), '/x')
    dconn = quantile_thresh(dconn, (1 - tie_density))

    return dconn


def quantile_thresh(mat: da.Array, quantile):
    d = mat.shape[-1]
    d = d * (d - 1) / 2  # upper triangle is considered only
    num = int((1 - quantile) * d)
    threshold = mat.flatten().topk(num)[0].to_hdf5(os.path.join(hd_path, 'tmpthr.hdf5'), '/x')
    print(threshold)
    mat = da.where(mat < threshold, 0, mat)
    mat.to_hdf5(os.path.join(hd_path, 'tmp1.hdf5'), '/x')

    return mat


def mat2pajek(mat: da.Array, output):
    with open(output, 'w') as out:
        # write vertices to pajek file
        shape = mat.shape
        out.write('*Vertices %s\n' % shape[-1])
        gen = ('%s "%s"\n' % (i, i) for i in range(1, shape[-1] + 1))
        out.writelines(gen)

        for i, row in enumerate(mat):
            for j, val in enumerate(row):
                if i < j:
                    out.writelines('%s %s %s\n' % (i, j, val))
                else:
                    continue


if __name__ == '__main__':
    import sys
    hd_path = sys.argv[1]
    dmat = os.path.join(hd_path, 'dmat.hdf5')
    rmat = os.path.join(hd_path, 'rmat.hdf5')
    if not os.path.exists(dmat):
        fake_dmat = da.random.normal(0, 30, size=(91282, 91282), chunks=(1000, 1000)).astype(np.float)
        fake_rmat = da.random.normal(0, 1, size=(91282, 91282), chunks=(1000, 1000)).astype(np.float)
        fake_dmat.to_hdf5(dmat, '/x')
        fake_rmat.to_hdf5(rmat, '/x')
    else:
        dmat = h5py.File(dmat)['/x']
        rmat = h5py.File(rmat)['/x']
        fake_dmat = da.from_array(dmat, chunks=(1000, 1000))
        fake_rmat = da.from_array(rmat, chunks=(1000, 1000))

    out = threshold_matrix(fake_rmat, fake_dmat, 15, 0.05)
    out.to_hdf5(os.path.join(hd_path, 'edge_matrix.hdf5'), '/edges')
    mat2pajek(out, 'pajek.net')
