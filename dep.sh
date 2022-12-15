DEP_PATH=`pwd`
export CGO_CFLAGS="-I$DEP_PATH/dep/include"
export CGO_LDFLAGS="-L$DEP_PATH/dep/lib -lleveldb -lsnappy"
export LD_LIBRARY_PATH=$DEP_PATH/dep/lib:$LD_LIBRARY_PATH
