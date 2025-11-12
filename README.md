# Telegram interactive bot (Telegram 双向机器人)

## 一、项目简介

本项目 Bot 是一 Telegram 消息转发机器人，具有丰富的功能，无任何广告。

### 主要特性

#### 基础功能
- **消息转发**：用户私聊消息自动转发到管理群组话题
- **双向通信**：管理员可在话题内回复用户
- **用户屏蔽**：支持通过开关话题屏蔽和解除屏蔽用户（屏蔽后你不会收到bot转发的对方消息，对方在给你发送消息后会收到当前屏蔽状况如临时或永久屏蔽的提醒）

#### 高级功能
- **话题管理**：为每个用户创建独立的管理话题
- **消息编辑同步**：用户和管理员的消息编辑实时同步
- **媒体组处理**：支持照片、视频等媒体组的转发
- **消息频率限制**：防止用户过于频繁发送消息
- **联系人卡片**：自动展示用户头像（如有）和直接联系方式
- **广播功能**：向所有活跃用户发送通知


## 二、准备工作

1. **获取 Bot Token**
   - 访问 [@BotFather](https://t.me/BotFather)
   - 发送 `/newbot` 创建机器人
   - 按提示设置机器人名称和用户名
   - 保存生成的 Token

2. **获取用户 ID**
   - 访问 [@username_to_id_bot](https://t.me/username_to_id_bot)
   - 获取你的用户 ID（管理员 ID）

3. **创建管理群组**
   - 创建一个新的 Telegram 群组
   - 将机器人添加到群组并设为管理员
   - 在群组设置中启用 "话题(Topics)" 功能
   - 获取群组 ID（可通过 [@username_to_id_bot](https://t.me/username_to_id_bot) 获取）


   ![image-20240703082929589](./doc/cn/image-20240703082929589.png)![image-20240703083040852](./doc/cn/image-20240703083040852.png)

## 三、部署运行

#### 3.1 服务器执行
可以参考 [频道](https://t.me/jiagesuxian/21)


#### 3.2 docker 执行（原作者项目）
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
- 编辑消息与部分其他功能由本人 [@horr_self]( https://t.me/horrorself_bot ) 添加

