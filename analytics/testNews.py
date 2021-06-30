import multiprocessing
from joblib import Parallel, delayed
from tqdm import tqdm

num_cores = multiprocessing.cpu_count()
inputs = tqdm(myList)

def my_function(myList):
    return np.sum([item ** 2 for item in myList])

if __name__ == "__main__":
    processed_list = Parallel(n_jobs=num_cores)(delayed(myfunction)(i,parameters) for i in inputs)
