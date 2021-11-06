this is just a scratchpad for my thoughts. not considered documentation

`main` does setup and then calls a function, let's call it `run`

`run`:
    - gets a list of `IngestSource`s, of various concrete types
    - combines them with `kmerge` into one iterator
    - `try_fold`s over that iterator
    - the `try_fold` closure calls `apply` on each produced `IngestObject`