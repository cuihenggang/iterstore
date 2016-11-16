# IterStore

[![License](https://img.shields.io/badge/license-BSD-blue.svg)](LICENSE)

IterStore is a parameter server library for iterative convergent
machine learning applications.
Our example applications include matrix factorization, LDA,
multi-class logistic regression, and PageRank.


## Build IterStore and example applications

If you use the Ubuntu 14.04 system, you can run the following command
to install the dependencies:

```
./scripts/install-deps-ubuntu14.sh
```

After installing the dependencies, you can build IterStore and our example applications by running this command:

```
scons -j8
```


## IterStore APIs

The detailed API descriptions can be found in include/iterstore.hpp and include/user-defined-types.hpp


## Reference Paper

Henggang Cui, Alexey Tumanov, Jinliang Wei, Lianghong Xu, Wei Dai, Jesse Haber-Kucharsky, Qirong Ho, Gregory R. Ganger, Phillip B. Gibbons, Garth A. Gibson, and Eric P. Xing.
[Exploiting Iterative-ness for Parallel ML Computations](https://users.ece.cmu.edu/~hengganc/archive/paper/[socc14]iterstore.pdf).
In ACM Symposium on Cloud Computing, 2014 (SoCC'14)
