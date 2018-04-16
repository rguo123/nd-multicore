import NeuroDataResource as ndr
import intern.utils.parallel as intern
import multiprocessing as mp
from util import Block
import sys
from importlib import import_module
import pickle
from functools import partial
import numpy as np


"""
    This function is designed to compute proper block sizes (less than 2 gb)
    when given a NDR
"""

def compute_blocks(resource, x_start = 0,
                             x_end = None,
                             y_start = 0,
                             y_end = None,
                             z_start = 0,
                             z_end = None):
    z, y, x = resource.max_dimensions

    if x_end == None:
        x_end = x
    if y_end == None:
        y_end = y
    if z_end == None:
        z_end = z

    block_size = (1000, 1000, 10)
    blocks = intern.block_compute(x_start, x_end, y_start, y_end, z_start, z_end, (0, 0, 0), block_size)
    ### IMPORTANT blocks are returned as x, y, z ###
    for i in range(len(blocks)):
        x_range, y_range, z_range = blocks[i]
        blocks[i] = Block(z_range, y_range, x_range)
    return blocks

def get_data(resource, block):
    y_range = [block.y_start, block.y_end]
    x_range = [block.x_start, block.x_end]
    z_range = [block.z_start, block.z_end]
    cutouts = {}

    for key in resource.requested_channels:
        if key in resource.channels:
            raw = resource.get_cutout(chan = key, zRange = z_range, yRange=y_range, xRange=x_range)
            cutouts[key] = raw
    block.data = cutouts
    return block

def job(block, resource, function = None):

    print("Starting job, retrieiving data")
    block = get_data(resource, block)
    print("Starting algorithm")
    try:
        result = function(block.data)
    except Exception as ex:
        print(ex)
        print("Ran into error in algorithm, exiting this block")
        return

    key = str(block.z_start) + "_" + str(block.y_start) + "_" + str(block.x_start)
    pickle.dump(result, open(key, "wb"))
    print("Done with job")
    return key

def run_parallel(config_file, cpus = None, function = None):
    ## Make resource and compute blocks
    resource = ndr.get_boss_resource(config_file)
    blocks = compute_blocks(resource)
    ## prepare job by fixing NeuroDataRresource argument
    task = partial(job, resource = resource, function = function)

    ## Prepare pool
    num_workers = cpus
    if num_workers is None:
        num_workers = mp.cpu_count() - 1
    pool = mp.Pool(num_workers)
    try:
        print(pool.map(task, blocks))
    except:
        pool.terminate()
        print("Parallel failed, closing pool and exiting")
        raise
    pool.terminate()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Provide module, function as arguments")
        sys.exit(-1)
    #TODO: integrate argparser
    mod = import_module(sys.argv[1])
    function = getattr(mod, sys.argv[2])
    run_parallel("neurodata.cfg", function = function)
