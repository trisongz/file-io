import time
import multiprocessing as mp
from tqdm.auto import tqdm

from . import logger, _enable_pbar

_cores = mp.cpu_count()

def create_multi_function(funct):
    return lambda x: funct(x)

def MultiThreadPipeline(data_iterator, funct, num_cores=None, fail_if=None):
    num_cores = num_cores or _cores
    pipeline_proc = create_multi_function(funct)

    def send_to_queue(input_q, iterator):
        for ex in iterator:
            input_q.put(ex)
        for _ in range(num_cores):
            input_q.put('Done')
    
    def process(input_q, output_q):
        while True:
            ex = input_q.get()
            if ex == 'Done':
                output_q.put('Done')
                break
            output_q.put(pipeline_proc(ex))
    
    start = time.time()
    pbar_iter = tqdm(data_iterator, desc=f'Processing using {num_cores} Cores', disable=(not _enable_pbar))
    input_q = mp.Queue(maxsize=num_cores)
    output_q = mp.Queue(maxsize=num_cores)
    gen_pool = mp.Pool(1, initializer=send_to_queue, initargs=(input_q, pbar_iter))
    pool = mp.Pool(num_cores, initializer=process, initargs=(input_q, output_q))
    finished_workers = 0
    valid_res, invalid_res = 0, 0
    
    while True:
        results = output_q.get()
        pbar_iter.update()
        if results == 'Done':
            finished_workers += 1
            if finished_workers == num_cores:
                total_res = valid_res + invalid_res
                valid_perc = valid_res / total_res * 100
                end = time.time() - start
                logger.info(f'Completed Processing {total_res} Items in {end / 60:.2f} mins\nValid: {valid_res} - {valid_perc:.2f}%\nInvalid: {invalid_res}')
                break
        
        else:
            if results == fail_if:
                invalid_res += 1
            else:
                valid_res += 1
                yield results




