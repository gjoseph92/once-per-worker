# once_per_worker

`once_per_worker` is a utility to create [dask.delayed](https://docs.dask.org/en/latest/delayed.html) objects around functions that you only want to ever run once per [distributed](https://distributed.dask.org/en/latest/) worker.

Say you have some large data baked into your docker image (so every worker already has a copy of it). You need to use that data as auxiliary input to another dask operation (`df.map_partitions`, for example). But parsing/preprocessing the data is slow, and once loaded into memory, the data is large, so it's faster for each worker to load the data from disk than to transfer the serialized data between workers in the cluster. So you only want to call the parsing function once per worker, then use the same parsed object per worker in all downstream tasks.

By wrapping your preprocessing function in `once_per_worker`, you get a Delayed object that, on a given worker, will always resolve to the same pre-processed object. Your function is called the first time you _access an attribute_ on the returned value.

## Example

```python
# example.py
import os
import time


class SlowDataLoader:
    def __init__(self, x):
        print(f"Very slowly loading the data on PID {os.getpid()}")
        time.sleep(x)
        self.x = x

    def value(self):
        print(f"SlowDataLoader is {hex(id(self))} on PID {os.getpid()}")
        return self.x

    def __getstate__(self):
        raise RuntimeError("Don't pickle me!")


if __name__ == "__main__":
    import dask
    import distributed
    from once_per_worker import once_per_worker

    ddf = dask.datasets.timeseries()

    loaded_slow_loader = once_per_worker(lambda: SlowDataLoader(3))
    # ^ this is a dask.Delayed object that will resolve to `SlowDataLoader(3)` on each worker

    ddf = ddf.map_partitions(
        lambda df, loader: df.assign(name=df.name + str(loader.value())),
        # ^ NOTE: `SlowDataLoader(3)` isn't created until the `loader.value` attribute access here
        loaded_slow_loader,
        meta=ddf,
    )

    client = distributed.Client(processes=True)
    print(client)
    print(ddf.compute())
```

```shell
$ python example.py
<Client: 'tcp://127.0.0.1:59309' processes=4 threads=16, memory=32.00 GiB>
Very slowly loading the data on PID 25799
Very slowly loading the data on PID 25800
Very slowly loading the data on PID 25801
Very slowly loading the data on PID 25802
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x1268e15e0 on PID 25800
SlowDataLoader is 0x1276becd0 on PID 25801
SlowDataLoader is 0x1276becd0 on PID 25801
SlowDataLoader is 0x1276becd0 on PID 25801
SlowDataLoader is 0x126aa7400 on PID 25802
SlowDataLoader is 0x126aa7400 on PID 25802
SlowDataLoader is 0x1268e15e0 on PID 25800
SlowDataLoader is 0x126aa7400 on PID 25802
SlowDataLoader is 0x1268e15e0 on PID 25800
SlowDataLoader is 0x1268e15e0 on PID 25800
SlowDataLoader is 0x126aa7400 on PID 25802
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x1276becd0 on PID 25801
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
SlowDataLoader is 0x11a8eed00 on PID 25799
                       id       name         x         y
timestamp
2000-01-01 00:00:00  1002     Jerry3 -0.094005  0.274997
2000-01-01 00:00:01   941  Patricia3 -0.246477  0.668154
2000-01-01 00:00:02   998       Bob3  0.255090  0.912544
2000-01-01 00:00:03  1013     Edith3  0.049940 -0.412688
2000-01-01 00:00:04  1008       Ray3  0.532393 -0.389245
...                   ...        ...       ...       ...
2000-01-30 23:59:55   997    Ingrid3  0.462019 -0.642151
2000-01-30 23:59:56  1054  Patricia3  0.030638  0.648196
2000-01-30 23:59:57  1062     Kevin3 -0.798188  0.480087
2000-01-30 23:59:58  1027    Ingrid3 -0.541842 -0.571846
2000-01-30 23:59:59   981     Jerry3 -0.564332 -0.217744
[2592000 rows x 4 columns]
```

## Installation

```
python -m pip install git+https://github.com/gjoseph92/once-per-worker.git
```

## Issues

This package is a bit of a hack.

* We don't take advantage of parallelism as much as possible: your delayed function isn't called until some other task accesses an attribute on it. Basically, we wait to call the function until right when it's needed, instead of calling it in advance so the data is already ready by the time it's needed.
* The data-loading function doesn't show up as its own task, so on the dashboard, whatever task accesses it will appear to take a very long time (because it's actually loading the data).
* You have to access an attribute on the object returned by your function for anything to actually happen.
