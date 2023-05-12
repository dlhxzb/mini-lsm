* MemTable: SkipMap<Bytes, Bytes>，自动按key排序，从头pop出转为L0 SSTable
  * memtable(r/w)写满后push进imm_memtables(r)，这里每次push的时候imm_memtables都pop，相当于imm容量只有短暂的1
* SSTable：存储文件中各个Block的Meta信息(first_key, offset)，给定key可定位block在文件偏移量，也可直接从cache读
  * l0_sstables: 存MemTable，与其它level还未merge，与它们的key可能有重复(删改)。可以看做MemTable持久化的临时level

查找方式：MemTable-跳表；SSTable-元信息定位到block，block内二分法查找

接下来实现Compaction这个压缩是指数据从level n -> n+1，与数据存储大小压缩不同，参照
[RocksDB Leveled Compaction](http://rocksdb.org.cn/doc/Compaction.html)