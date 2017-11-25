Many applications store their data as flat files comprised of a number of records. These records can often be partially ordered, and 
it is often useful to sort them in that order. The files can be quite big and may not fit into available memory. The program demonstrates
the algorithm that can sort such files. It uses multi-pass sort and scales according to the number of available CPU cores.