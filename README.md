Hello Claude IA, implement the following functionality using Python 3, use colours for output and emojis. Use tqdm
library for reporting progress. For storing the status, save it as compressed json, being careful about storing hashes
as strings and mtimes as floats rendered into strings. Only one tqdm per command should be used. Files can have spaces
so be careful with regexes.

Try to make readable code and reuse functions. No need to add spurious comments.

# filesets

Tool to keep track of big datasets of files, particularly useful when datasets are replicated and might be partially
synchronised.

## Configuration file

```
$ cat some.fsc
algo=murmur128
/mnt/raid1/main => /main/
"mnt/raid2/second folder" => /second/
```

At the moment algo is fixed but it might change in the future, real path to virtual paths associations are distinguished
with the `=>` symbol.

## Typical usage

`fileset some.fsc sync`
It will scan the filesystems and store the file `some.fsd` with all files and its hashes, modified files they will be
reported, if files are missing they will be reported too. No file can be missing on the first run when there is no
database yet. It reports on each file and its hash and estimated time to finish. Example output:

```
fileset some.fsc syncing ...
Sync'ing files: 50%|   | 20/234 [00:15<00:00 300files/s]
b504978962569ce992d8369ec6290635ab212331 folder/file2 [CHANGED!]
Sync'ing files: 51%|   | 20/234 [00:15<00:00 300files/s]
[...]
123 Files found
100 Files added
20 Files already existed
3 Files changed
```

if c^C is pressed, it will finish gracefully and save the state up to the moment it was interrupted.
A file is considered to be changed if the hash has changed.

`fileset some.fsc sync fast`
It will only re-hash the files that the mtime or size have changed compared to the values stored before in the database.
or the files that have no hash yet.

`fileset some.fsc status`
Prints a status report of data recoded, it will not check the filesystem, it will list among other things: total files
in the filesystem, in the database, deleted files (as in existing in the database but not in the filesystem anymore),
added files (the other way around: missing from the database, but existing in the filesystem), and modified files
(different mtime or size).

`fileset some.fsc check`
Prints a status report mentioned earlier, and proceeds to re-check file hashes, starting from the oldest checked first.
The data file is not updated. Multiple runs of 'check' will return the same result, only after a 'sync' the data file
will be updated with the new data.

`fileset some.fsc check 10%`
Only checks 10% of the file hashes.

`fileset some.fsd diff other.fsd`
Compares two different fileset data records (notice the `.fsd` instead of `.fsc`), and prints the differences.
Useful for verifying replication status of big datasets and tools like `rsync` can not be used because the datasets are
spread across multiple filesystems, or it is unfeasible to hash multiple times massive amount of tiles (i.e. 100's TiB)

`fileset help` (or just no parameters)
Displays a comprehensive command line use for each command.
