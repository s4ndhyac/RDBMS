# RDBMS - A fully functional relational database management system

### Description

The system consists of 4 broad layers of interface:

- The Paged file system and Record Based File Manager [PFM and RBFM](project1_report.txt)
- The Relational Manager [RM](project2_report.txt)
- The Indexing Engine [IX](project3_report.txt)
- The Query Engine [QE](project4_report.txt)

### Build Instructions

- Modify the "CODEROOT" variable in makefile.inc to point to the root of your codebase. Usually, this is not necessary.
- `cd` into the respective folder of the layer `rbf`, `rm`, `ix` or `qe` and build with `make clean && make`
- Each folder corresponding to a layer has test cases along with it. Execute them by running `./rbftest_01` etc.
