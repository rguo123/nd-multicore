import numpy as np
import intern.utils.parallel as intern


## Block class to organize the data at the every end. Not needed but nice to have
class Block:
    def __init__(self, z_range, y_range, x_range):
        self.x_start = x_range[0]
        self.x_end = x_range[1]
        self.y_start = y_range[0]
        self.y_end = y_range[1]
        self.z_start = z_range[0]
        self.z_end = z_range[1]
        self.data = None

## merge data, does not handle padding
def merge(img_blocks, orig_shape):
    block_list = []
    for key, value in img_blocks.items():
        block_list.append(Block(key, value))
    merged_array = np.zeros(orig_shape)
    for block in block_list:
        merged_array[block.z_start:block.z_end, block.y_start:block.y_end, block.x_start:block.x_end, :] = block.data
    return merged_array


#data should be numpy array, block_size should be a tuple with 3 elements in it in order (z, y, x)
def split_data(data, block_size):
    z, y, x = data.shape[0:3]
    block_size = block_size[::-1]
    blocks = intern.block_compute(0, x, 0, z, 0, y, (0, 0, 0), block_size)

    img_blocks = {}
    for block in blocks:
        z_range, y_range, x_range = block
        img_block = img[z_range[0]:z_range[1], y_range[0]:y_range[1], x_range[0]:x_range[1], :]
        ## give label
        block_id = str(z_range[0]) + "_" + str(y_range[0]) + "_" + str(x_range[0])
        img_blocks[block_id] = img_block
    return img_blocks


## padding is also a tuple (x, y, z)
def split_data_padded(data, block_size, padding):
    z, y, x = data.shape[0:3]
    block_size = block_size[::-1]
    blocks = intern.block_compute(0, x, 0, z, 0, y, (0, 0, 0), block_size)

    img_blocks = {}
    for block in blocks:
        z_range, y_range, x_range = block
        ## annoying code to add padding, oh wellz
        z_start, z_end = z_range
        x_start, x_end = x_range
        y_start, y_end = y_range

        if z_start - padding[0] >= 0:
            z_start -= padding[0]
        if z_end + padding[0] < z:
            z_end += padding[0]

        if x_start - padding[2] >= 0:
            x_start -= padding[2]
        if x_end + padding[2] < x:
            x_end += padding[2]

        if y_start - padding[1] >= 0:
            y_start -= padding[1]
        if y_end + padding[1] < y:
            y_end += padding[1]


        img_block = img[z_start:z_end, y_start:y_end, x_start:x_end, :]
        ## give label
        block_id = str(z_start) + "_" + str(y_start) + "_" + str(x_start)
        img_blocks[block_id] = img_block
    return img_blocks


def format_data_to_cube(data_dict):
    data = []
    for chan, value in data_dict.items():
        data.append(value)
    data = np.stack(data)
    ## Verify data is correct for user
    print("Data shape is:\t" + str(data.shape))
    return data
