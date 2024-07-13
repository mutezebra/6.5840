# 简介
由于代码阅读难度不大，而且配有详细的注释，故没有特别的文档来讲解代码。这里给出使用示例

## LAB1
`coordinator`
> $ cd src/main  
> $ rm mr-*  
> $ go run mrcoordinator.go pg*.txt  

`worker`  当然,如果你想的话，可以启用多个worker，
但你需要让worker启动之后休眠几秒钟，不然有可能另
一个worker启动之前任务就已经完成了
> $ cd src/main  
> $ rm mr-*  
> $ go build -buildmode=plugin ../mrapps/wc.go  
> $ go run mrworker.go wc.so  

等待程序结束之后可以输入命令查看结果
> $ cat mr-out-* | sort | more

## LAB2
> $ cd src/kvsrv  
> $ go test -race  
