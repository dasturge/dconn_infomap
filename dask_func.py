import dask.array as da
import numpy as np


def threshold_matrix(dconn: da.Array, dmat, min_dist, tie_density, chunksize='auto'):
    dconn = da.triu(dconn, k=1)
    dconn = da.where(dmat<min_dist, 0, dconn)
    dconn.to_hdf5('tmp4.hdf5', '/x')
    dconn = quantile_thresh(dconn, (1 - tie_density))
    dconn.to_hdf5('tmp3.hdf5', '/x')

    return dconn


def quantile_thresh(mat: da.Array, quantile):
    d = mat.shape[-1]
    d = d * (d - 1) / 2  # upper triangle is considered only
    num = int((1 - quantile) * d)
    threshold = mat.flatten().topk(num)[0]
    mat = da.where(mat<threshold, 0, mat)
    mat.to_hdf5('tmp')

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
                out.writelines('%s %s %s\n' % (i, j, val))


if __name__ == '__main__':
    chunksize = 1000000
    fake_dmat = da.random.normal(0, 30, size=(91282, 91282), chunks=(1000, 1000))
    fake_rmat = da.random.normal(0, 1, size=(91282, 91282), chunks=(1000, 1000))
    fake_dmat.to_hdf5('tmp2.hdf5', '/x')
    fake_rmat.to_hdf5('tmp1.hdf5', '/x')

    out = threshold_matrix(fake_rmat, fake_dmat, 15, 0.05)
    out.to_hdf5()
    mat2pajek(out, 'pajek.net')
