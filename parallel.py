import NeuroDataResource as ndr
import intern.utils.parallel as intern
import multiprocessing as mp
from util import Block
import sys
from importlib import import_module
from functools import partial
import numpy as np


'''
    This function is designed to compute proper block sizes (less than 2 gb)
    when given a NDR

    Input:
        Resource    NeuroDataResource class containing necessary parameters
        block_size  (x, y, z) specifying size of blocks
'''
def compute_blocks(resource, block_size):
    z_max, y_max, x_max = resource.max_dimensions

    x_start, x_end = resource.x_range
    y_start, y_end = resource.y_range
    z_start, z_end = resource.z_range

    blocks = intern.block_compute(x_start, x_end, y_start, y_end, z_start, z_end, (0, 0, 0), block_size)
    ### IMPORTANT blocks are returned as x, y, z ###
    for i in range(len(blocks)):
        x_range, y_range, z_range = blocks[i]
        # create Block object to preserve original location of block
        blocks[i] = Block(z_range, y_range, x_range)
    return blocks


'''
    This function pulls data from BOSS based off Block object

    Input:
        resource NeuroDataResource object
        block Block object
    Output:
        Block object populated with data
            The data lies in block.data and is a dictionary where each key
            is a channel, and each value is raw data from BOSS
'''
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

'''
    This function pulls data from BOSS, and runs a function on it

    Input:
        block Block object without raw data
        resource NeuroDataResource object
        function pipeline to be run on data
    Output:
        String of block key (z_start, y_start, x_start)

    # NOTE: if you want to save output or merge, that's on you
    # TODO: second module with TSQ merging
'''
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
    print("Done with job")
    return key

'''
    This is the main driver function to start multiprocessing

    Input:
        config_file Neurodata config file
        function function to be run, must take in Data Dictionary!
        cpus number of cpus to use
        block_size size of blocks
'''
def run_parallel(config_file, function, cpus = None, block_size = (1000, 1000, 10)):
    ## Make resource and compute blocks
    resource = ndr.get_boss_resource(config_file)
    blocks = compute_blocks(resource, block_size)
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
    run_parallel(config_file = "neurodata.cfg", function = function)
