import os

import nibabel as nib
import numpy as np


def volume_dist_mat(mask):
    data = nib.load(mask)
    x = 1


def combine_geo_with_mat(geo, mat):
    pass


if __name__ == '__main__':
    mask = os.path.join(os.path.realpath(os.path.dirname(__file__)),
                        'Atlas_ROIs.2.nii.gz')
    mat = volume_dist_mat(mask)
