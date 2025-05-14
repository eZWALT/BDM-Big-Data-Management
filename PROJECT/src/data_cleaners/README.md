## Data Cleaners

**Author:** Marc Parcerisa

This folder contains a bunch of spark jobs that are responsible for reading the
files from the landing zone Delta Lake tables, cleaning them, creating an
extraction of all the data and loading it into the trusted zone Delta Lake
tables (overriding it), ensuring that data is in the correct format, that all
columns are present, and, most importantly, that there are no duplicates.

The trusted zone is another Delta Lake storage area where data is supposed
to be more stable and reliable. The definition of this in our context is
that data is not duplicated, and all columns are present.

