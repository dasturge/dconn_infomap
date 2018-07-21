#!/usr/bin/env python3
import argparse

from helpers import iterate_dconn


def _cli():
    parser = generate_parser()
    args = parser.parse_args()
    return interface(
        args.dconn,
        args.perc_thresh,
        args.dist_thresh,
        args.dmat,
        args.tie_density,
    )


def generate_parser(parser=None):
    if not parser:
        parser = argparse.ArgumentParser(
            prog='run.py',
            description="""Wraps nibabel, infomap, and workbench to take a 
            dense connectivity matrix and run the infomap algorithm on 
            weighted edges.  Outputs communities.""",
            epilog="""example: 
            ./run.py -d 15 -t .01 -m geo_distances.mat --min-network-size 400 
            --min-region-size 30 study_average.dconn.nii 
            /home/users/me/study_infomap""",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
    parser.add_argument('dconn', metavar='DCONN_FILENAME',
                        help='input dense connectivity matrix.')
    parser.add_argument('output', metavar='OUTPUT_DIRECTORY')
    parser.add_argument('--dist-thresh', '-d', type=int,
                        help='minimum distance exclusion threshold.')
    parser.add_argument('--tie-density', '-t', type=float,
                        help='ratio of top connections to include.')
    parser.add_argument('--dmat', '-m',
                        help='geodesic distance matrix for the brain.  '
                             'Defaults to Conte69.')
    parser.add_argument('--min-network-size')
    parser.add_argument('--min-region-size')

    return parser


def interface(dconn_filename, perc_thresh, dist_thresh, dmat, tie_density):

    # create pajek format node/edge specification
    dconn = iterate_dconn(dconn_filename)


if __name__ == '__main__':
    _cli()
