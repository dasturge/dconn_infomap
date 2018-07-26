import os
import subprocess
import tempfile
import time

import h5py
import nibabel
import numpy as np


def niiquantile(filename, quantile):
    if quantile < .5:
        print('no need for lower threshold implementation as of yet')
        raise NotImplementedError

    # get number of values for in-memory quantile computation.
    shape = get_shape(filename)
    num_values = np.product(shape)
    chunksize = int(num_values * (1 - quantile))
    approx_step = int(chunksize / np.product(shape[:-1]) / 2 + 1)
    gen = chunk_iter_dconn(filename, chunksize, approx_step)

    # initialize sorted array
    print('reading initial %s values from dconn (%s of total values).' % (
        chunksize, (1 - quantile)))
    t1 = time.perf_counter()
    top = next(gen)
    top.sort()
    t = time.perf_counter() - t1
    print('read and sorted initial values in %s seconds' % int(t + 1))

    # in sets of chunksize, mergesort
    print('reading and comparing values in sets of %s' % chunksize)
    for i, vals in enumerate(gen):
        top = np.concatenate((top, vals))
        del vals
        top.sort(kind='mergesort')
        top = top[chunksize:]
        t = time.perf_counter() - t1
        print('about %s%% done. %s seconds passed'
              % (int((1 - quantile) * (i + 1) * 100), int(t + 1)))
    t = time.perf_counter() - t1
    print('calculated %s quantile of %s numbers in %s total seconds' % (
        quantile, num_values, int(t + 1)))

    return top[0]  # return lowest value


def iterate_dconn(filename, step=1):
    nii = nibabel.load(filename)
    for idx in range(0, nii.shape[-1], step):
        yield nii.dataobj[..., idx:idx+step].astype(np.float).flatten()


def chunk_iter_dconn(filename, chunksize, stepsize=1):
    dconn = iterate_dconn(filename, stepsize)
    prev = np.array([], dtype=np.float)
    for row in dconn:
        prev = np.concatenate((prev, row))
        if prev.size >= chunksize:
            yield prev[:chunksize]
            prev = prev[chunksize:]
    yield prev


def get_shape(filename):
    nii = nibabel.load(filename)
    return nii.shape


def create_pajek(rmat, dmat, tie_density, dist_thresh, output):

    with open(output, 'w') as out:
        # write vertices to pajek file
        shape = get_shape(rmat)
        out.write('*Vertices %s\n' % shape[-1])
        gen = ('%s "%s"\n' % (i, i) for i in range(1, shape[-1] + 1))
        out.writelines(gen)

        # compute edge weight threshold on disk for data too big for mem
        thresh = niiquantile(rmat, (1 - tie_density))

        t = tempfile.TemporaryFile()
        dmat = [x for x in h5py.File(dmat, 'r').values()][0]
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
            gen = ('%s %s %.6f\n' % (idx, i, round(v, 6))
                   for i, v in enumerate(array) if v)
            t.writelines(gen)

        # find total number of edges then append temporary file.
        t.seek(0)
        for i, l in enumerate(t):
            pass
        edges = i + 1
        t.seek(0)
        out.write('*Edges %s\n' % edges)
        for line in t:
            out.write(line)
        t.close()


def run_infomap(pajek, output_folder, attempts=5, seed=0):
    out_log = os.path.join(output_folder, 'infomap.out')
    err_log = os.path.join(output_folder, 'infomap.err')
    stdout, _ = subprocess.Popen('which infomap', shell=True,
                              stdout=subprocess.PIPE).communicate()
    print('using infomap binary: %s' % str(stdout))
    cmd = ' '.join(['infomap', pajek, output_folder, '--clu', '-i', 'pajek',
                    '-u', '-s', str(seed), '-N', str(attempts)])
    with open(out_log, 'w') as out, open(err_log, 'w') as err:
        subprocess.run(cmd, shell=True, stdout=out, stderr=err)


def clu_to_ciftis(clu_file):
    pass


if __name__ == '__main__':
    import sys
    filename = sys.argv[1]
    dmat = sys.argv[2]
    create_pajek(filename, dmat, .005, 15, 'test.pajek')
    run_infomap('test.pajek', os.path.dirname(__file__))
