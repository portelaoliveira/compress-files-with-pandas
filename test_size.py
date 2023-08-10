from pathlib import Path
import pandas as pd
import time

file_path = Path("annual-enterprise.csv")
# file_path = Path("selected-services-march-2023-quarter-csv.csv")
df = pd.read_csv(file_path)

sizes = [dict(name=file_path.name, size=file_path.stat().st_size, time=0)]

for p in [".csv.gz", ".csv.xz", ".csv.bz2", ".csv.zip"]:
    path = file_path.with_suffix(p)
    path.unlink(missing_ok=True)
    start = time.time()
    df.to_csv(path, index=False)
    sizes.append(
        {
            "name": path.name,
            "size": path.stat().st_size,
            "time": int((time.time() - start) * 1000),
        }
    )

for p in [".pkl", ".pkl.gz", ".pkl.xz", ".pkl.bz2"]:
    path = file_path.with_suffix(p)
    path.unlink(missing_ok=True)
    start = time.time()
    df.to_pickle(path)
    sizes.append(
        {
            "name": path.name,
            "size": path.stat().st_size,
            "time": int((time.time() - start) * 1000),
        }
    )

for p in [".xls", ".xlsx"]:
    path = file_path.with_suffix(p)
    path.unlink(missing_ok=True)
    start = time.time()
    df.to_excel(path, index=False)
    sizes.append(
        {
            "name": path.name,
            "size": path.stat().st_size,
            "time": int((time.time() - start) * 1000),
        }
    )

for p in [".dta", ".dta.gz", ".dta.xz", ".dta.bz2"]:
    path = file_path.with_suffix(p)
    path.unlink(missing_ok=True)
    start = time.time()
    df.to_stata(path)
    sizes.append(
        {
            "name": path.name,
            "size": path.stat().st_size,
            "time": int((time.time() - start) * 1000),
        }
    )

for p in [".hdf"]:
    path = file_path.with_suffix(p)
    path.unlink(missing_ok=True)
    start = time.time()
    df.to_hdf(path, "df", index=False)
    sizes.append(
        {
            "name": path.name,
            "size": path.stat().st_size,
            "time": int((time.time() - start) * 1000),
        }
    )

cr = pd.DataFrame(sizes)
cr["ratio"] = cr["size"] / cr.loc[0, "size"]
cr.sort_values("ratio", inplace=True)

print(cr)
