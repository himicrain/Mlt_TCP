# Mlt_TCP

多线程 服务器，基于线程池 ， 处理键值对

只有服务器端，客户端可以通过telnet链接

eg.

#8080 代表数据端口， 8000 代表控制端口

./server 8080 8000

telnet 127.0.0.1 8080

/*
 * Parse a data command. Legal commands:
 * PUT key text
 * GET key
 * COUNT
 * DELETE key
 * EXISTS key
 */

