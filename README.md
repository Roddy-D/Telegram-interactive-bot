# Telegram interactive bot (Telegram 双向机器人)

## 一、简介
Telegram的开源双向机器人。避免垃圾信息


### 特色
- 当客户通过机器人联系客服时，所有消息将被完整转发到后台管理群组，生成一个独立的以客户信息命名子论坛，用来和其他客户区分开来。
- 客服在子论坛中的回复，可以直接回复给客户。
- 开启消息编辑功能 ———— forked


## 二、准备工作
本机器人的主要原理是将客户和机器人的对话，转发到一个群内（自用，最好是私有群），并归纳每个客户的消息到一个子版块。
所以，在开工前，你需要：
1. 找 @BotFather 申请一个机器人。

2. 获取机器人的token

3. 建立一个群组（按需设置是否公开）

4. 群组的“话题功能”打开。

5. 将自己的机器人，拉入群组。提升权限为管理员。

6. 管理权限切记包含`消息管理`，`话题管理`。

7. 通过机器人 @GetTheirIDBot 获取群组的内置ID和管理员用户ID。

   ![image-20240703082929589](./doc/cn/image-20240703082929589.png)![image-20240703083040852](./doc/cn/image-20240703083040852.png)

## 三、部署运行

#### 3.1 服务器执行
可以参考 [博客] (https://blog.922768.xyz/topicgrambot/)


#### 3.2 docker 执行
1. 安装docker ， 参看 [Install Docker under Ubuntu 22.04](https://gist.github.com/dehsilvadeveloper/c3bdf0f4cdcc5c177e2fe9be671820c7)
2. 执行`docker build -t tgibot .` 生成一个tgibot的镜像
3. 执行`docker run --restart always --name telegram-interactive-bot  -v "$PWD":/app tgibot:latest` 生成容器并执行。


# 关于

- 本产品基于Apache协议开源。
- 作者 米哈( [@MrMiHa](https://t.me/MrMiHa) )是一个苦逼程序员，不是煤场奴工，有问题别太理直气壮的跑来下命令。
- 讨论群组是 : https://t.me/DeveloperTeamGroup 欢迎加入后玩耍
- 随意Fork，记得保留`关于`的内容。
- 初版写了2小时。喜欢请打赏。不会部署，群里找我。
- 服务器推荐RackNerd的。实际上，我也确实用这个。够便宜。这款就够：[2核3G--年32刀](https://my.racknerd.com/aff.php?aff=11705&pid=905) 
- 实在搞不定部署，可以群里找大家帮忙部署下。服务器也可以找大家共用： https://t.me/DeveloperTeamGroup 
- 实在实在实在搞不定部署，找  [@MrMiHa](https://t.me/MrMiHa)  同学付费部署……
- 编辑消息功能由本人 ( https://t.me/horrorself_bot ) 添加

