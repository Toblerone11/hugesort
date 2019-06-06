import os
import tempfile
import logging

import pickle
import heapq
from multiprocessing import Process

from itertools import islice, groupby
import math

DEFAULT_BUFSIZE = 8192


def get_new_file(buffersize=None):
    if buffersize is None:
        buffersize = DEFAULT_BUFSIZE
    return  tempfile.TemporaryFile('w+b', buffering=buffersize)


def dumptofile(a, f, chunksize=1):  # dump the array to file, as pickled data
    n_chunks = math.ceil(float(len(a)) / chunksize)
    pickle.dump(n_chunks, f)
    # pickle.dump(a, f)
    a_it = iter(a)
    for chunk in (list(islice(a_it, chunksize)) for _ in range(n_chunks)):
        pickle.dump(chunk, f)
    f.flush()


def feedfile(f):
    f.seek(0)
    n_chunks = pickle.load(f)
    for _ in range(n_chunks):
        yield from pickle.load(f)


def dumpsort(a, f, sortkw, i=None, chunksize=1):
    logging.debug(f"Sorting {i}")
    a.sort(**sortkw)
    logging.debug(f"Dumping {i} to tmp file")
    dumptofile(a, f, chunksize)
    del a[:]
    del a


def get_add_file(tmpfiles, buffersize=None):
    new_f = get_new_file(buffersize=buffersize)
    tmpfiles.append(new_f)
    return new_f


def hugesort(iterable, key=None, reverse=False, subsize=100_000, filebuffering=None, chunksize=1):
    temp_files = []
    a = []
    array_count = 0
    processes = []
    # pool = Pool(7)
    end_of_array = False
    while not end_of_array:
        logging.debug(f"Fetching the next array: {array_count}")
        array_count += 1
        a = list(islice(iterable, subsize))

        if len(a) == 0:
            logging.info("Finish iterating the full array")
            end_of_array = True # just in case
            break

        f = get_add_file(temp_files, buffersize=filebuffering)
        p = Process(target=dumpsort, args=(a, f, dict(key=key, reverse=reverse), array_count, chunksize))
        p.start()
        processes.append(p)
        
    for p in processes:
        p.join()


    files_iter = [feedfile(fd) for fd in temp_files]
    logging.info("Merging from tmp files")
    yield from heapq.merge(*files_iter, key=key, reverse=reverse)

    for tmp_f in temp_files:
        tmp_f.close()


if __name__ == "__main__":

    logging.basicConfig(format='%(asctime)s - %(levelname)s:%(message)s',
    level=logging.INFO, datefmt='%m/%d/%Y %I:%M:%S %p')

    import random
    a = (random.randint(0, 100_000) for _ in range(1_000_000))
    res = list(hugesort(a, key=lambda i: i, reverse=True, subsize=10_000, chunksize=1000))
    # res = [(k, len(list(g))) for k, g in
    #                    groupby(islice(res, 100), key=lambda i: i)]
    print(len(res))
    # print(res)
    print(res[:100])
    print(res[-100:])

    







