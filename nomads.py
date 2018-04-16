import numpy as np
from skimage.measure import label
from skimage.filters import threshold_otsu
from scipy.ndimage.filters import convolve
from skimage.measure import block_reduce as pool


def compute_convolutional_cov(vol1, vol2, kernel_shape):
    mu_kernel = np.ones(kernel_shape)/float(np.sum(np.ones(kernel_shape)))
    e1 = convolve(vol1, mu_kernel)
    e2 = convolve(vol2, mu_kernel)

    e12 = convolve(np.log(np.exp(vol1)+np.exp(vol2)), mu_kernel)

    cov = e12 - np.log(np.exp(e1) + np.exp(e2))

    return cov


def remove_low_volume_predictions(label_img, thresh):
    keep_list = []
    for idx in np.unique(label_img):
        if not idx == 0:
            if(np.sum(label_img == idx)) > thresh:
                keep_list.append(idx)

    return np.isin(label_img, keep_list)


def z_transform(img):
    sigma = np.std(img)
    mu = np.average(img)
    return (img - mu)/sigma


def normalize_data(data):
    return np.stack([z_transform(chan) for chan in data])


def predict_from_feature_map(feature_map):
    foreground = feature_map > threshold_otsu(feature_map)
    predictions = label(foreground)
    return predictions


def format_data(data_dict):
    data = []
    for chan, value in data_dict.items():
        format_chan = []
        for z in range(value.shape[0]):
            raw = value[z]
            if (raw.dtype != np.dtype("uint8")):
                info = np.iinfo(raw.dtype) # Get the information of the incoming image type
                raw = raw.astype(np.float64) / info.max # normalize the data to 0 - 1
                raw = 255 * raw # Now scale by 255
                raw = raw.astype(np.uint8)
            raw = pool(raw, (36, 36), np.mean)
            format_chan.append(raw)
        data.append(np.stack(format_chan))
    data = np.stack(data)
    return data


def pipeline(input_data, verbose=False):
    data = format_data(input_data)
    if verbose:
        print('Normalizing Data')
    normed_data = normalize_data(data)
    if verbose:
        print('Generating Covariance Map')
    cov_map = compute_convolutional_cov(normed_data[0],
                                        normed_data[1],
                                        (3, 3, 3))

    if verbose:
        print('Binarizing Covariance Map')
    predictions = predict_from_feature_map(cov_map)

    if verbose:
        print('Pruning Predictions')
    filtered_predictions = remove_low_volume_predictions(predictions, 30)

    return filtered_predictions
