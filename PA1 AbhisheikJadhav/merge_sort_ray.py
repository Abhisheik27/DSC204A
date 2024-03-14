ray.shutdown()
import heapq
from typing import List
import ray
from ray import ObjectRef
from plain_merge_sort import plain_merge_sort
import time
import numpy as np

## RAY INIT. DO NOT MODIFY
num_workers = 4
ray.init(num_cpus=num_workers)
## END OF INIT

## Feel free to add your own functions here for usage with Ray

@ray.remote
def plain_merge_sort_ray(collection_ref, start, end):
    # No need to call ray.get here; it will be called inside the remote function
    sublist = collection_ref[start:end]  # This slicing will be on the list, not on the ObjectRef
    return plain_merge_sort(sublist)

def merge(sublists: List[list]) -> list:
    """
    Merge sorted sublists into a single sorted list.

    :param sublists: List of sorted lists
    :return: Merged result
    """
    ## YOU CAN MODIFY THIS WITH RAY
    result = []
    sublists = [sublist for sublist in sublists if len(sublist)> 0]
    heap = [(sublist[0], i, 0) for i, sublist in enumerate(sublists)]
    heapq.heapify(heap)
    while len(heap):
        val, i, list_ind = heapq.heappop(heap)
        result.append(val)
        if list_ind+1 < len(sublists[i]):
            heapq.heappush(heap, (sublists[i][list_ind+1], i, list_ind+1))
    return result


def merge_sort_ray(collection_ref: ObjectRef, length: int, npartitions: int = 4) -> list:
    """
    Merge sort with ray
    """
    ## DO NOT MODIFY: START    
    breaks = [i*length//npartitions for i in range(npartitions)]
    breaks.append(length)
    # Keep track of partition end points
    sublist_end_points = [(breaks[i], breaks[i+1]) for i in range(len(breaks)-1)]
    ## DO NOT MODIFY: END
    
    ## PLEASE COMPLETE THIS ##
    sorted_sublists_refs = [
        plain_merge_sort_ray.remote(collection_ref, start, end)
        for start, end in sublist_end_points
    ]
    
    # Wait for all sorting tasks to complete and retrieve the results
    sorted_sublists = ray.get(sorted_sublists_refs) 
    ## END ##
    # Pass your list of sorted sublists to merge
    return merge(sorted_sublists)

if __name__ == "__main__":
    # We will be testing your code for a list of size 10M. Feel free to edit this for debugging. 
    list1 = list(np.random.randint(low=0, high=1000, size=10000000))
    list2 = [c for c in list1] # make a copy
    length = len(list2)
    list2_ref = ray.put(list2) # insert into the driver's object store
    
    start1 = time.time()
    list1 = plain_merge_sort(list1, npartitions=num_workers)
    end1 = time.time()
    time_baseline = end1 - start1
    print("Plain sorting:", time_baseline)

    start2 = time.time()
    list2 = merge_sort_ray(collection_ref=list2_ref, length=length, npartitions=num_workers)
    end2 = time.time()
    time_ray = end2 - start2
    print("Ray sorting:", time_ray)

    print("Speedup: ", time_baseline/ time_ray)
    ## You can uncomment and verify that this holds
    assert sorted(list1) == list2, "Sorted lists are not equal"