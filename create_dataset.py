import numpy as np
import pandas as pd
import xarray as xr
import dask.array as da

import random
import string

def generate_unique_strings(num_strings, string_length):
    # Check if the requested number of unique strings is feasible
    max_possible_unique_strings = 26 ** string_length
    if num_strings > max_possible_unique_strings:
        raise ValueError(f"Cannot generate {num_strings} unique strings of length {string_length}. Maximum possible unique strings for this length is {max_possible_unique_strings}.")
    # Generate unique strings
    unique_strings = set()
    while len(unique_strings) < num_strings:
        random_string = ''.join(random.choices(string.ascii_uppercase, k=string_length))
        unique_strings.add(random_string)
    return list(unique_strings)
    

def main():
	
	# create a mock timeseries xarray dataset, save as zarr
	# target:
	# Number of times: ~10,000
	# Number of issues: 15,000
	random.seed(17)
	np.random.seed(17)

	times = pd.date_range("1990-01-01", "2020-01-01", name="time", freq="D")  # 10958 entries
	issues = sorted(generate_unique_strings(15000, 3))
	field = 'AA'
	
	# Initialize as empty dask arrays
	data_vars = {}
	data_vars[field] = (["time", "issue"], da.zeros((len(times), len(issues)), chunks=(1000,1000), dtype="int32"))

	ds = xr.Dataset(
		coords={
			"time": times,
			"issue": issues,
		},
		data_vars=data_vars,
	)
	
	print("initializing zarr")
	zarr_file = "timeseries.zarr"
	ds.to_zarr(zarr_file)
	
	ds = xr.open_zarr(zarr_file)
	
	# Now fill in with actual data in steps
	print("writing field: ", field)
	data_vars = {}
	data_vars[field] = (["time", "issue"], np.random.randint(0, 100000000, size=(len(times), len(issues))).astype('int32'))
	tmp_ds = xr.Dataset(
		coords={
			"time": times,
			"issue": issues,
		},
		data_vars=data_vars,
	)
	tmp_df = pd.DataFrame(data_vars[field][1],index=times,columns=issues)
		
	# write to zarr 
	tmp_ds.to_zarr(zarr_file, region={"time": slice(0,len(times)), "issue": slice(0,len(issues))})
	
if __name__ == "__main__":
    main()
