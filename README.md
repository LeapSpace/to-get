# to-get
## 断点续传 主要适用于网络环境较差，连接经常中断的环境
## Usage：
```shell
python main.py https://github.com/xxxx/xxx.tar.gz
```
python 3.7

实现类中支持设置下载参数：

block_size: 单块大小

pool_size: 下载线程数

dst_dir: 下载文件保存目录

dst_name: 下载文件保存到本地文件名

懒得写参数解析了，将就着用吧，实在需要的自己改代码🐶