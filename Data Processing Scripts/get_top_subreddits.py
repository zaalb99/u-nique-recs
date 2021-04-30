from subprocess import SubprocessError
import time
import os
from typing import OrderedDict
import psutil
import multiprocessing as mp
import gc
import json
from collections import OrderedDict
import pickle

def chunkify_file(fname, size=1024*1024*1000, skiplines=-1):
    """
    function to divide a large text file into chunks each having size ~= size so that the chunks are line aligned

    Params : 
        fname : path to the file to be chunked
        size : size of each chink is ~> this
        skiplines : number of lines in the begining to skip, -1 means don't skip any lines
    Returns : 
        start and end position of chunks in Bytes
    """
    chunks = []
    fileEnd = os.path.getsize(fname)
    with open(fname, "rb") as f:
        if(skiplines > 0):
            for i in range(skiplines):
                f.readline()

        chunkEnd = f.tell()
        count = 0
        while True:
            chunkStart = chunkEnd
            f.seek(f.tell() + size, os.SEEK_SET)
            f.readline()  # make this chunk line aligned
            chunkEnd = f.tell()
            chunks.append((chunkStart, chunkEnd - chunkStart, fname))
            count+=1

            if chunkEnd > fileEnd:
                break
    return chunks

def parallel_apply_line_by_line_chunk(chunk_data):
    """
    function to apply a function to each line in a chunk

    Params :
        chunk_data : the data for this chunk 
    Returns :
        list of the non-None results for this chunk
    """
    chunk_start, chunk_size, file_path, func_apply = chunk_data[:4]
    func_args = chunk_data[4:]

    t1 = time.time()
    ret_list = list()
    filter_list = set(pickle.load( open( "filter_subs.p", "rb" ) ))
    with open(file_path, "rb") as f:
        f.seek(chunk_start)
        cont = f.read(chunk_size).decode(encoding='utf-8')
        lines = cont.splitlines()

        for i,line in enumerate(lines):
            ret = func_apply(line,filter_list,*func_args)

            if(ret != None):
                ret_list.append(ret)
    
    return ret_list

def parallel_apply_line_by_line(input_file_path, chunk_size_factor, num_procs, skiplines, func_apply, func_args, fout=None):
    """
    function to apply a supplied function line by line in parallel

    Params :
        input_file_path : path to input file
        chunk_size_factor : size of 1 chunk in MB
        num_procs : number of parallel processes to spawn, max used is num of available cores - 1
        skiplines : number of top lines to skip while processing
        func_apply : a function which expects a line and outputs None for lines we don't want processed
        func_args : arguments to function func_apply
        fout : do we want to output the processed lines to a file
    Returns :
        list of the non-None results obtained be processing each line
    """
    num_parallel = min(num_procs, psutil.cpu_count()) - 1

    jobs = chunkify_file(input_file_path, 1024 * 1024 * chunk_size_factor, skiplines)

    jobs = [list(x) + [func_apply] + func_args for x in jobs]

    print("Starting the parallel pool for {} jobs ".format(len(jobs)))

    lines_counter = 0

    pool = mp.Pool(num_parallel, maxtasksperchild=1000)  # maxtaskperchild - if not supplied some weird happend and memory blows as the processes keep on lingering

    outputs = []
    subreddits = set()
    for i in range(0, len(jobs), num_parallel):
        print("Chunk start = ", i)
        t1 = time.time()
        chunk_outputs = pool.map(parallel_apply_line_by_line_chunk, jobs[i : i + num_parallel])

        for i, subl in enumerate(chunk_outputs):
            outputs += subl
        del(chunk_outputs)
        gc.collect()
        print("All Done in time ", time.time() - t1)

    pool.close()
    pool.terminate()
    return outputs


def process_line(line, filter_list):
    try:
        output = json.loads(line)
    except json.decoder.JSONDecodeError:
        return None

    if output['display_name'] not in filter_list and output['subscribers'] is not None:
        return (output['display_name'], output['subscribers'])
    else:
        return None


def run():
    input_file_path = "reddit_subreddits.json"
    outputs = parallel_apply_line_by_line(input_file_path, 100, 8, 0, process_line, [], fout=None)

    outputs.sort(reverse=True, key=lambda x: x[1])

    for i in range(10):
        print("{} - {} {}".format(i, outputs[i][0], outputs[i][1]))

    pickle.dump(outputs, open( "top_subs.p", "wb" ))


if __name__ == '__main__':
    run()
    

    
