import numpy as np
from intern.remote.boss import BossRemote
from intern.resource.boss.resource import ChannelResource, ExperimentResource, CoordinateFrameResource
import configparser

class NeuroDataResource:
    def __init__(self, host, token, collection, experiment, requested_channels):
        self._bossRemote = BossRemote({'protocol': 'https',
                                       'host': host,
                                       'token': token})
        self.collection = collection
        self.experiment = experiment
        if len(requested_channels) == 0:
            self.requested_channels = self.channels
        else:
            self.requested_channels = requested_channels
        self.channels = self._bossRemote.list_channels(collection, experiment)
        self._get_coord_frame_details()


    def _get_coord_frame_details(self):
        exp_resource = ExperimentResource(self.experiment, self.collection)
        coord_frame = self._bossRemote.get_project(exp_resource).coord_frame

        coord_frame_resource = CoordinateFrameResource(coord_frame)
        data = self._bossRemote.get_project(coord_frame_resource)

        self.max_dimensions = (data.z_stop, data.y_stop, data.x_stop)
        self.voxel_size = (data.z_voxel_size, data.y_voxel_size, data.x_voxel_size)


    def _get_channel(self, chan_name):
        """
        Helper that gets a fully initialized ChannelResource for an *existing* channel.
        Args:
            chan_name (str): Name of channel.
            coll_name (str): Name of channel's collection.
            exp_name (str): Name of channel's experiment.
        Returns:
            (intern.resource.boss.ChannelResource)
        """
        chan = ChannelResource(chan_name, self.collection, self.experiment)
        return self._bossRemote.get_project(chan)

    def assert_channel_exists(self, channel):
        return channel in self.channels

    def get_cutout(self, chan, zRange=None, yRange=None, xRange=None):
        if chan not in self.channels:
            print('Error: Channel Not Found in this Resource')
            return
        if zRange is None or yRange is None or xRange is None:
            print('Error: You must supply zRange, yRange, xRange kwargs in list format')
            return

        channel_resource = self._get_channel(chan)
        datatype = channel_resource.datatype

        data = self._bossRemote.get_cutout(channel_resource,
                                           0,
                                           xRange,
                                           yRange,
                                           zRange)

        #Datatype check. Recast to original datatype if data is float64
        if data.dtype == datatype:
            return data
        else:
            return data.astype(datatype)

'''
    Parses .cfg files for BOSS metadata
'''
def get_boss_config(boss_config_file):
    config = configparser.ConfigParser()
    config.read(boss_config_file)

    remote_metadata = {}
    remote_metadata["token"] = config['Default']['token']
    remote_metadata["host"] = config['Default']['host']
    remote_metadata["experiment"] = config['Parallel']['experiment']
    remote_metadata["collection"] = config['Parallel']['collection']

    channels = config["Parallel"]["channels"]
    channels = channels.split(",")
    remote_metadata["channels"] = channels
    return remote_metadata

'''
    Instantiate resource
'''
def get_boss_resource(config_file):
    config = get_boss_config(config_file)
    resource = create_resource(config)
    return resource

def create_resource(config):
    resource = NeuroDataResource(config["host"], config["token"], config["collection"], config["experiment"], config["channels"])
    return resource
