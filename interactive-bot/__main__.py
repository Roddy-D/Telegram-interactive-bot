import os
import random
import time
import asyncio
from datetime import datetime, timedelta
from string import ascii_letters as letters

import httpx
import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
# 导入常量，用于过滤器
from telegram.constants import ChatType, UpdateType
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    PicklePersistence,
    filters,
)
from telegram.helpers import mention_html

from db.database import SessionMaker, engine
from db.model import Base, FormnStatus, MediaGroupMesssage, MessageMap, User

from . import (
    admin_group_id,
    admin_user_ids,
    app_name,
    bot_token,
    is_delete_topic_as_ban_forever,
    is_delete_user_messages,
    logger,
    welcome_message,
    disable_captcha,
    message_interval,
    enable_math_verification,
)
from .utils import delete_message_later

# 创建表
Base.metadata.create_all(bind=engine)
db = SessionMaker()


# 延时发送媒体组消息的回调 (保持不变)
async def _send_media_group_later(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    media_group_id = job.data
    _, from_chat_id, target_id, dir = job.name.split("_")

    media_group_msgs = (
        db.query(MediaGroupMesssage)
        .filter(
            MediaGroupMesssage.media_group_id == media_group_id,
            MediaGroupMesssage.chat_id == from_chat_id,
        )
        .all()
    )
    if not media_group_msgs: # 如果找不到消息组，则退出
        logger.warning(f"Media group {media_group_id} not found in DB for job {job.name}")
        return
    try:
        chat = await context.bot.get_chat(target_id)
        if dir == "u2a":
            u = db.query(User).filter(User.user_id == from_chat_id).first()
            if not u or not u.message_thread_id: # 确保用户和话题存在
                logger.warning(f"User {from_chat_id} or their topic not found for media group {media_group_id}")
                return
            message_thread_id = u.message_thread_id
            sents = await chat.send_copies(
                from_chat_id,
                [m.message_id for m in media_group_msgs],
                message_thread_id=message_thread_id,
            )
            for sent, msg in zip(sents, media_group_msgs):
                msg_map = MessageMap(
                    user_chat_message_id=msg.message_id,
                    group_chat_message_id=sent.message_id,
                    user_id=u.user_id,
                )
                db.add(msg_map)
            db.commit() # 提交数据库更改
        else: # a2u
            sents = await chat.send_copies(
                from_chat_id, [m.message_id for m in media_group_msgs]
            )
            for sent, msg in zip(sents, media_group_msgs):
                msg_map = MessageMap(
                    user_chat_message_id=sent.message_id,
                    group_chat_message_id=msg.message_id,
                    user_id=target_id, # target_id 在 a2u 时就是 user_id
                )
                db.add(msg_map)
            db.commit() # 提交数据库更改
    except BadRequest as e:
        logger.error(f"Error sending media group {media_group_id} in job {job.name}: {e}")
        # 可以考虑在这里通知管理员或用户发送失败
    except Exception as e:
        logger.error(f"Unexpected error in _send_media_group_later for job {job.name}: {e}", exc_info=True)


# 延时发送媒体组消息 (保持不变)
async def send_media_group_later(
    delay: float,
    chat_id,
    target_id,
    media_group_id: int,
    dir,
    context: ContextTypes.DEFAULT_TYPE,
):
    name = f"sendmediagroup_{chat_id}_{target_id}_{dir}"
    # 移除同名的旧任务，防止重复执行
    current_jobs = context.job_queue.get_jobs_by_name(name)
    for job in current_jobs:
        job.schedule_removal()
        logger.debug(f"Removed previous job with name {name}")
    # 添加新任务
    context.job_queue.run_once(
        _send_media_group_later, delay, chat_id=chat_id, name=name, data=media_group_id
    )
    logger.debug(f"Scheduled media group {media_group_id} sending job: {name} in {delay}s")
    return name


# 更新用户数据库 (保持不变)
def update_user_db(user: telegram.User):
    if db.query(User).filter(User.user_id == user.id).first():
        return
    u = User(
        user_id=user.id,
        first_name=user.first_name or "未知", # 处理 first_name 可能为 None 的情况
        last_name=user.last_name,
        username=user.username,
    )
    db.add(u)
    db.commit()


# 发送联系人卡片 (修正版)
async def send_contact_card(
    chat_id, message_thread_id, user: User, update: Update, context: ContextTypes
):
    try:
        # === 修改 1: 使用 user.user_id 获取头像 ===
        user_photo = await context.bot.get_user_profile_photos(user.user_id, limit=1)

        if user_photo.total_count > 0:
            pic = user_photo.photos[0][-1].file_id
            await context.bot.send_photo(
                chat_id,
                photo=pic,
                # === 修改 2 & 3: 使用 user.user_id 生成文本 ===
                caption=f"👤 {mention_html(user.user_id, user.first_name or str(user.user_id))}\n\n📱 {user.user_id}\n\n🔗 直接联系：{f'@{user.username}' if user.username else f'tg://user?id={user.user_id}'}",
                message_thread_id=message_thread_id,
                parse_mode="HTML",
            )
        else:
            # 如果没有头像，可以只发送文本信息或者使用 send_message
            await context.bot.send_message(
                chat_id,
                # === 修改 4 & 5: 使用 user.user_id 生成文本 ===
                text=f"👤 {mention_html(user.user_id, user.first_name or str(user.user_id))}\n\n📱 {user.user_id}\n\n🔗 直接联系：{f'@{user.username}' if user.username else f'tg://user?id={user.user_id}'}",
                message_thread_id=message_thread_id,
                parse_mode="HTML",
            )
    except Exception as e:
         # === 修改 6: 日志中使用 user.user_id ===
         logger.error(f"Failed to send contact card for user {user.user_id} to chat {chat_id}: {e}")

# start 命令处理 (你修改后的版本)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    update_user_db(user)
    if user.id in admin_user_ids:
        logger.info(f"{user.first_name}({user.id}) is admin")
        try:
            bg = await context.bot.get_chat(admin_group_id)
            if bg.type == "supergroup" and bg.is_forum: # 确保是开启了话题的超级群组
                logger.info(f"Admin group is {bg.title}")
                await update.message.reply_html(
                    f"你好管理员 {mention_html(user.id, user.full_name)} ({user.id})\n\n欢迎使用 {app_name} 机器人。\n\n目前你的配置正确，机器人已在群组 <b>{bg.title}</b> 中。请确保机器人拥有在话题中发送消息的权限。"
                )
            else:
                 logger.warning(f"Admin group {admin_group_id} is not a supergroup with topics enabled.")
                 await update.message.reply_html(
                    f"⚠️⚠️后台管理群组设置错误⚠️⚠️\n管理员 {mention_html(user.id, user.full_name)}，群组 ID (`{admin_group_id}`) 对应的必须是一个已启用“话题(Topics)”功能的超级群组。请检查群组设置和配置中的 `admin_group_id`。"
                )
        except BadRequest as e:
            logger.error(f"Admin group error (BadRequest): {e}")
            await update.message.reply_html(
                 f"⚠️⚠️无法访问后台管理群组⚠️⚠️\n管理员 {mention_html(user.id, user.full_name)}，无法获取群组信息。请确保机器人已被邀请加入群组 (`{admin_group_id}`) 并且具有必要权限（至少需要发送消息权限）。\n错误细节：{e}"
            )
        except Exception as e:
            logger.error(f"Admin group check error: {e}", exc_info=True)
            await update.message.reply_html(
                f"⚠️⚠️检查后台管理群组时发生意外错误⚠️⚠️\n管理员 {mention_html(user.id, user.full_name)}，请查看日志了解详情。\n错误细节：{e}"
            )
        # return ConversationHandler.END # 不应在 start 命令中结束会话
    else:
        # 非管理员用户
        # 如果启用了数学验证码且用户未验证，立即发送验证码
        if enable_math_verification and not context.user_data.get("math_verified", False):
            # 生成验证码
            challenge = generate_math_verification_challenge()
            context.user_data["math_verification_challenge"] = challenge['challenge']
            context.user_data["math_verification_answer"] = challenge['answer']
            context.user_data["math_verification_offset"] = challenge['offset']
            context.user_data["math_verification_attempts"] = 0
            
            # 发送验证码消息
            await update.message.reply_html(
                f"👋 {mention_html(user.id, user.full_name)}，欢迎使用！\n\n"
                f"🔐 验证码：四位数 <b>{challenge['challenge']}</b> 的每一位数字加上 <b>{challenge['offset']}</b>（超过9取个位数）\n\n"
                f"⚠️ 请输入4位数字答案\n\n"
                f"👋 {mention_html(user.id, user.full_name)}, Welcome!\n\n"
                f"🔐 Verification: Each digit of <b>{challenge['challenge']}</b> plus <b>{challenge['offset']}</b> (if over 9, keep ones digit)\n\n"
                f"⚠️ Please enter 4-digit answer"
            )
        else:
            # 已验证或未启用验证码，显示欢迎消息
            await update.message.reply_html(
                f"{mention_html(user.id, user.full_name)}：\n\n{welcome_message}"
            )


# 人机验证 (保持不变，但注意路径)
async def check_human(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    # 注意: ./assets/imgs 路径相对于脚本执行的当前工作目录
    img_dir = "./assets/imgs"
    if not os.path.isdir(img_dir) or not os.listdir(img_dir):
        logger.warning(f"Captcha image directory '{img_dir}' not found or empty. Skipping check_human.")
        context.user_data["is_human"] = True # 无法验证，暂时跳过
        return True

    if not context.user_data.get("is_human", False): # 检查是否已经验证通过
        if context.user_data.get("is_human_error_time", 0) > time.time() - 120:
            # 2分钟内禁言
            sent_msg = await update.message.reply_html("你因验证码错误已被临时禁言，请 2 分钟后再试。\nYou have been temporarily muted due to captcha error, please try again in 2 minutes.")
            await delete_message_later(10, sent_msg.chat.id, sent_msg.message_id, context) # 10秒后删除提示
            await delete_message_later(5, update.message.chat.id, update.message.message_id, context) # 5秒后删除用户消息
            return False

        try:
            file_name = random.choice(os.listdir(img_dir))
            code = file_name.replace("image_", "").replace(".png", "")
            file_path = os.path.join(img_dir, file_name) # 使用 os.path.join 兼容不同系统

            # 简单的验证码字符集，避免难以辨认的字符
            valid_letters = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789" # 移除了 I, O, 0, 1
            codes = ["".join(random.sample(valid_letters, len(code))) for _ in range(7)]
            codes.append(code)
            random.shuffle(codes)

            # 尝试从 bot_data 获取缓存的文件 ID
            photo_file_id = context.bot_data.get(f"image|{code}")

            # 准备按钮
            buttons = [
                InlineKeyboardButton(x, callback_data=f"vcode_{x}_{user.id}") for x in codes
            ]
            # 每行最多4个按钮
            button_matrix = [buttons[i : i + 4] for i in range(0, len(buttons), 4)]

            captcha_message = f"{mention_html(user.id, user.first_name or str(user.id))}，请在 60 秒内点击图片中显示的验证码。回答错误将导致临时禁言。\n{mention_html(user.id, user.first_name or str(user.id))}, please click the captcha shown in the image within 60 seconds. Wrong answers will result in temporary muting."

            if photo_file_id:
                # 如果有缓存，直接用 file_id 发送
                sent = await update.message.reply_photo(
                    photo=photo_file_id,
                    caption=captcha_message,
                    reply_markup=InlineKeyboardMarkup(button_matrix),
                    parse_mode="HTML",
                )
            else:
                # 如果没有缓存，发送文件并获取 file_id
                sent = await update.message.reply_photo(
                    photo=open(file_path, "rb"), # 以二进制读取方式打开文件
                    caption=captcha_message,
                    reply_markup=InlineKeyboardMarkup(button_matrix),
                    parse_mode="HTML",
                )
                # 缓存 file_id 以便下次使用
                biggest_photo = sorted(sent.photo, key=lambda x: x.file_size, reverse=True)[0]
                context.bot_data[f"image|{code}"] = biggest_photo.file_id
                logger.debug(f"Cached captcha image file_id for code {code}")

            # 存储正确的验证码以便后续检查
            context.user_data["vcode"] = code
            context.user_data["vcode_message_id"] = sent.message_id # 存储验证码消息ID
            # 60秒后删除验证码图片消息
            await delete_message_later(60, sent.chat.id, sent.message_id, context)
            # 5秒后删除用户的原始触发消息 (可选)
            await delete_message_later(5, update.message.chat.id, update.message.message_id, context)

            return False # 需要用户验证
        except FileNotFoundError:
             logger.error(f"Captcha image file not found: {file_path}")
             await update.message.reply_html("抱歉，验证码图片丢失，请稍后再试或联系对方。\nSorry, the captcha image is missing, please try again later or contact him.")
             context.user_data["is_human"] = True # 暂时跳过
             return True
        except IndexError:
            logger.error(f"Captcha image directory '{img_dir}' seems empty.")
            await update.message.reply_html("抱歉，无法加载验证码，请稍后再试或联系对方。\nSorry, unable to load captcha, please try again later or contact him.")
            context.user_data["is_human"] = True # 暂时跳过
            return True
        except Exception as e:
             logger.error(f"Error during check_human: {e}", exc_info=True)
             await update.message.reply_html("抱歉，验证过程中发生错误，请稍后再试。\nSorry, an error occurred during verification, please try again later.")
             context.user_data["is_human"] = True # 暂时跳过
             return True

    return True # 已验证


# 生成数学验证码挑战和答案
def generate_math_verification_challenge():
    """生成一个4位数字和一个加数（offset），用户需要计算每位数字加上offset后的结果"""
    # 随机生成四位数字
    challenge_digits = ''.join([str(random.randint(0, 9)) for _ in range(4)])
    
    # 随机生成加数（1-9，避免0没有意义）
    offset = random.randint(1, 9)
    
    # 计算正确答案
    answer = ''.join([str((int(d) + offset) % 10) for d in challenge_digits])
    
    return {
        'challenge': challenge_digits,
        'answer': answer,
        'offset': offset
    }


# 数学验证码验证
async def check_math_verification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    检查用户的数学验证码
    返回 True: 已验证，允许转发消息
    返回 False: 未验证或验证中，不转发消息
    """
    user = update.effective_user
    message = update.message
    
    # 如果已经验证通过，直接返回 True，允许转发消息
    if context.user_data.get("math_verified", False):
        return True
    
    # 检查是否因验证失败而被临时禁言
    if context.user_data.get("math_verification_banned_until", 0) > time.time():
        time_left = int(context.user_data["math_verification_banned_until"] - time.time())
        sent_msg = await message.reply_html(
            f"🚫 验证错误过多，请 {time_left} 秒后再试\n"
            f"🚫 Too many errors, try again in {time_left} seconds"
        )
        await delete_message_later(10, sent_msg.chat.id, sent_msg.message_id, context)
        await delete_message_later(5, message.chat.id, message.message_id, context)
        return False
    
    # 检查是否已达到最大尝试次数（10次）
    total_attempts = context.user_data.get("math_verification_attempts", 0)
    if total_attempts >= 10:
        sent_msg = await message.reply_html(
            "❌ 验证失败10次，已禁止使用\n"
            "❌ 10 failed attempts, access denied"
        )
        await delete_message_later(30, sent_msg.chat.id, sent_msg.message_id, context)
        await delete_message_later(5, message.chat.id, message.message_id, context)
        return False
    
    # 如果还没有发送验证挑战，生成新的验证码
    if not context.user_data.get("math_verification_challenge"):
        challenge = generate_math_verification_challenge()
        context.user_data["math_verification_challenge"] = challenge['challenge']
        context.user_data["math_verification_answer"] = challenge['answer']
        context.user_data["math_verification_offset"] = challenge['offset']
        context.user_data["math_verification_attempts"] = 0
    
    # 获取当前的验证码信息
    current_challenge = context.user_data.get("math_verification_challenge")
    current_offset = context.user_data.get("math_verification_offset")
    
    # 用户发送的内容
    user_answer = message.text.strip() if message.text else ""
    
    # 如果不是4位数字，显示验证码题目并提示
    if not user_answer or not user_answer.isdigit() or len(user_answer) != 4:
        sent_msg = await message.reply_html(
            f"🔐 验证码：四位数 <b>{current_challenge}</b> 的每一位数字加上 <b>{current_offset}</b>（超过9取个位数）\n\n"
            f"⚠️ 请输入4位数字答案\n\n"
            f"🔐 Verification: Each digit of <b>{current_challenge}</b> plus <b>{current_offset}</b> (if over 9, keep ones digit)\n\n"
            f"⚠️ Please enter 4-digit answer"
        )
        await delete_message_later(60, sent_msg.chat.id, sent_msg.message_id, context)
        await delete_message_later(5, message.chat.id, message.message_id, context)
        return False
    
    correct_answer = context.user_data.get("math_verification_answer")
    
    # 验证答案
    if user_answer == correct_answer:
        # 验证成功
        context.user_data["math_verified"] = True
        context.user_data.pop("math_verification_challenge", None)
        context.user_data.pop("math_verification_answer", None)
        context.user_data.pop("math_verification_offset", None)
        context.user_data.pop("math_verification_attempts", None)
        context.user_data.pop("math_verification_banned_until", None)
        
        await message.reply_html(
            "✅ 验证成功！\n✅ Verification successful!"
        )
        
        # 发送欢迎消息（与未启用验证码时的格式完全一致）
        await message.reply_html(
            f"{mention_html(user.id, user.full_name)}：\n\n{welcome_message}"
        )
        
        # 删除验证码答案消息，因为这不是用户真正想发送的内容
        await delete_message_later(3, message.chat.id, message.message_id, context)
        return False  # 不转发验证码答案，让用户重新发送真正的消息
    else:
        # 验证失败，增加尝试次数
        new_total_attempts = total_attempts + 1
        context.user_data["math_verification_attempts"] = new_total_attempts
        
        # 如果达到上限，禁止使用
        if new_total_attempts >= 10:
            sent_msg = await message.reply_html(
                "❌ 验证失败已达上限（10次），已禁止使用\n"
                "❌ Maximum attempts reached (10), access denied"
            )
            await delete_message_later(30, sent_msg.chat.id, sent_msg.message_id, context)
            await delete_message_later(5, message.chat.id, message.message_id, context)
            return False
        
        # 重新生成新的验证码
        challenge = generate_math_verification_challenge()
        context.user_data["math_verification_challenge"] = challenge['challenge']
        context.user_data["math_verification_answer"] = challenge['answer']
        context.user_data["math_verification_offset"] = challenge['offset']
        
        sent_msg = await message.reply_html(
            f"❌ 验证失败（{new_total_attempts}/10）\n\n"
            f"🔐 新验证码：四位数 <b>{challenge['challenge']}</b> 的每一位数字加上 <b>{challenge['offset']}</b>（超过9取个位数）\n\n"
            f"❌ Failed ({new_total_attempts}/10)\n\n"
            f"🔐 New code: Each digit of <b>{challenge['challenge']}</b> plus <b>{challenge['offset']}</b> (if over 9, keep ones digit)"
        )
        await delete_message_later(60, sent_msg.chat.id, sent_msg.message_id, context)
        await delete_message_later(5, message.chat.id, message.message_id, context)
        return False


# 处理验证码回调 (改进)
async def callback_query_vcode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    try:
        _, code_clicked, target_user_id_str = query.data.split("_")
    except ValueError:
        logger.warning(f"Invalid vcode callback data format: {query.data}")
        await query.answer("无效操作。\nInvalid operation.", show_alert=True)
        return

    if target_user_id_str != str(user.id):
        # 不是发给这个用户的验证码
        await query.answer("这不是给你的验证码哦。\nThis captcha is not for you.", show_alert=True)
        return

    # 从 user_data 获取正确的验证码和消息 ID
    correct_code = context.user_data.get("vcode")
    vcode_message_id = context.user_data.get("vcode_message_id")

    # 检查验证码是否存在或已过期 (被删除)
    if not correct_code or not vcode_message_id:
        await query.answer("验证已过期或已完成。\nVerification has expired or been completed.", show_alert=True)
        # 尝试删除可能残留的旧验证码消息
        if query.message and query.message.message_id == vcode_message_id:
             try:
                 await query.message.delete()
             except BadRequest:
                 pass # 消息可能已被删除
        return

    # 防止重复点击或处理旧消息
    if query.message and query.message.message_id != vcode_message_id:
        await query.answer("此验证码已失效。\nThis captcha is no longer valid.", show_alert=True)
        return


    if code_clicked == correct_code:
        # 点击正确
        await query.answer("✅ 验证成功！\n✅ Verification successful!", show_alert=False)
        # 发送欢迎消息
        await context.bot.send_message(
            user.id, # 直接发送给用户
            f"🎉 {mention_html(user.id, user.first_name or str(user.id))}，验证通过，现在可以开始对话了！\n🎉 {mention_html(user.id, user.first_name or str(user.id))}, verification passed, you can now start chatting!",
            parse_mode="HTML",
        )
        context.user_data["is_human"] = True
        # 清理 user_data 中的验证码信息
        context.user_data.pop("vcode", None)
        context.user_data.pop("vcode_message_id", None)
        context.user_data.pop("is_human_error_time", None) # 清除错误时间
        # 删除验证码消息
        try:
            await query.message.delete()
        except BadRequest:
            pass # 消息可能已被删除或过期
    else:
        # 点击错误
        await query.answer("❌ 验证码错误！请等待 2 分钟后再试。\n❌ Captcha error! Please wait 2 minutes before trying again.", show_alert=True)
        context.user_data["is_human_error_time"] = time.time() # 记录错误时间
        # 清理验证码信息，强制用户下次重新获取
        context.user_data.pop("vcode", None)
        context.user_data.pop("vcode_message_id", None)
        # 删除验证码消息
        try:
            await query.message.delete()
        except BadRequest:
             pass # 消息可能已被删除或过期

# 转发消息 u2a (用户到管理员)
async def forwarding_message_u2a(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message # 确保使用 update.message

    # 1. 数学验证码验证 (如果启用，优先于图片验证码)
    if enable_math_verification:
        if not await check_math_verification(update, context):
            return # 未通过验证则中止
    
    # 2. 图片人机验证 (如果启用且未启用数学验证)
    elif not disable_captcha:
        if not await check_human(update, context):
            return # 未通过验证则中止

    # 3. 消息频率限制 (如果启用)
    if message_interval > 0: # 仅在设置了间隔时检查
        current_time = time.time()
        last_message_time = context.user_data.get("last_message_time", 0)
        if current_time < last_message_time + message_interval:
            time_left = round(last_message_time + message_interval - current_time)
            # 只在剩余时间大于 0 时提示
            if time_left > 0:
                reply_msg = await message.reply_html(f"发送消息过于频繁，请等待 {time_left} 秒后再试。\nSending messages too frequently, please wait {time_left} seconds before trying again.")
                await delete_message_later(5, reply_msg.chat_id, reply_msg.message_id, context)
                await delete_message_later(3, message.chat.id, message.message_id, context) # 删除用户过快的消息
            return # 中止处理
        context.user_data["last_message_time"] = current_time # 更新最后发送时间

    # 4. 更新用户信息
    update_user_db(user)

    # 5. 获取用户和话题信息
    u = db.query(User).filter(User.user_id == user.id).first()
    if not u: # 理论上 update_user_db 后应该存在，但加个保险
        logger.error(f"User {user.id} not found in DB after update_user_db call.")
        await message.reply_html("发生内部错误，无法处理您的消息。\nAn internal error occurred and your message cannot be processed.")
        return
    message_thread_id = u.message_thread_id
    # 6. 检查话题状态
    topic_status = "opened" # 默认状态
    if message_thread_id:
        f_status = db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).first()
        if f_status and f_status.status == "closed":
            topic_status = "closed"
            await message.reply_html("对话已被对方关闭。您的消息暂时无法送达。如需继续，请等待或请求对方重新打开对话。\nThe conversation has been closed by him. Your message cannot be delivered temporarily. If you need to continue, please wait or ask him to reopen the conversation.")
            return # 如果话题关闭，则不转发

    # 7. 如果没有话题ID，创建新话题
    if not message_thread_id or topic_status == "closed": # 如果话题被非永久删除关闭，也视为需要重开（根据逻辑决定）
        # 如果 !is_delete_topic_as_ban_forever 且 topic_status == "closed"，理论上不应到这里，但作为保险
        if topic_status == "closed" and is_delete_topic_as_ban_forever:
            return # 确认不再处理

        try:
            # 使用你修改后的话题名称格式
            topic_name = f"{user.full_name}|{user.id}"
            # 限制话题名称长度 (Telegram API 限制 128 字符)
            topic_name = topic_name[:128]
            forum_topic = await context.bot.create_forum_topic(
                admin_group_id,
                name=topic_name,
            )
            message_thread_id = forum_topic.message_thread_id
            u.message_thread_id = message_thread_id
            db.add(u)
            # 记录新话题状态
            new_f_status = FormnStatus(message_thread_id=message_thread_id, status="opened")
            db.add(new_f_status)
            db.commit()
            logger.info(f"Created new topic {message_thread_id} for user {user.id} ({user.full_name})")

            # 发送欢迎和联系人卡片到新话题
            await context.bot.send_message(
                admin_group_id,
                f"🆕 新的用户 {mention_html(user.id, user.full_name)} ({user.id}) 发起了新的对话。",
                message_thread_id=message_thread_id,
                parse_mode="HTML",
            )
            await send_contact_card(admin_group_id, message_thread_id, u, update, context)

        except BadRequest as e:
             logger.error(f"Failed to create topic for user {user.id}: {e}")
             await message.reply_html(f"创建会话失败，请稍后再试或联系对方。\nFailed to create session, please try again later or contact him.\nError: {e}")
             return
        except Exception as e:
             logger.error(f"Unexpected error creating topic for user {user.id}: {e}", exc_info=True)
             await message.reply_html("创建会话时发生未知错误。\nAn unknown error occurred while creating the session.")
             return

    # 8. 每日首次消息回执
    try:
        today_str = datetime.now().strftime("%Y-%m-%d")
        last_ack_date = context.user_data.get("last_ack_date")
        if last_ack_date != today_str:
            ack_msg = await message.reply_text("您的消息已送达\nYour message has been delivered")
            context.user_data["last_ack_date"] = today_str
            # 10 秒后自动删除回执
            await delete_message_later(3, ack_msg.chat.id, ack_msg.message_id, context)
    except Exception as e:
        logger.warning(f"Failed to send daily ack to user {user.id}: {e}")

    # 9. 准备转发参数
    params = {"message_thread_id": message_thread_id}
    if message.reply_to_message:
        reply_in_user_chat = message.reply_to_message.message_id
        msg_map = db.query(MessageMap).filter(MessageMap.user_chat_message_id == reply_in_user_chat).first()
        if msg_map and msg_map.group_chat_message_id:
            params["reply_to_message_id"] = msg_map.group_chat_message_id
        else:
            logger.debug(f"Original message for reply {reply_in_user_chat} not found in group map.")
            # 可以选择不引用，或者通知用户无法引用

    # 10. 处理转发逻辑 (包括媒体组)
    try:
        if message.media_group_id:
            # 处理媒体组
            # 检查这条消息是否是这个媒体组的第一条带标题的消息
            existing_media_group = db.query(MediaGroupMesssage).filter(
                MediaGroupMesssage.media_group_id == message.media_group_id,
                MediaGroupMesssage.chat_id == message.chat.id
            ).first()

            # 将当前消息存入媒体组消息表
            msg = MediaGroupMesssage(
                chat_id=message.chat.id,
                message_id=message.message_id,
                media_group_id=message.media_group_id,
                is_header=not existing_media_group, # 第一条消息作为header
                caption_html=message.caption_html if not existing_media_group else None, # 只有header存caption
            )
            db.add(msg)
            db.commit()

            # 只有当这是该媒体组的第一条消息时，才安排延迟发送任务
            if not existing_media_group:
                logger.debug(f"Received first message of media group {message.media_group_id} from user {user.id}")
                await send_media_group_later(
                    3, # 延迟3秒发送媒体组
                    user.id,
                    admin_group_id,
                    message.media_group_id,
                    "u2a",
                    context
                )
            else:
                 logger.debug(f"Received subsequent message of media group {message.media_group_id} from user {user.id}")

        else:
            # 处理单条消息
            chat = await context.bot.get_chat(admin_group_id) # 目标是管理群组
            sent_msg = await chat.send_copy(
                from_chat_id=message.chat.id, # 来源是用户私聊
                message_id=message.message_id,
                **params # 包括 thread_id 和可能的 reply_to_message_id
            )
            # 记录消息映射
            msg_map = MessageMap(
                user_chat_message_id=message.id,
                group_chat_message_id=sent_msg.message_id,
                user_id=user.id,
            )
            db.add(msg_map)
            db.commit()
            logger.debug(f"Forwarded u2a: user({user.id}) msg({message.id}) -> group msg({sent_msg.message_id}) in topic({message_thread_id})")
    except BadRequest as e:
            logger.warning(f"Failed to forward message u2a (user: {user.id}, topic: {message_thread_id}): {e}")
            # === 修改开始: 修正 if 条件 ===
            # 使用 .lower() 进行大小写不敏感比较
            error_text = str(e).lower()
            if "message thread not found" in error_text or "topic deleted" in error_text or ("chat not found" in error_text and str(admin_group_id) in error_text):
            # === 修改结束: 修正 if 条件 ===
                original_thread_id = u.message_thread_id # 保存旧 ID 用于日志和清理
                logger.info(f"Topic {original_thread_id} seems deleted. Cleared thread_id for user {user.id}.")
                # 清理数据库
                u.message_thread_id = None # 使用 None 更标准
                db.add(u)
                db.query(FormnStatus).filter(FormnStatus.message_thread_id == original_thread_id).delete()
                db.commit()
                # 检查是否允许重开话题
                if not is_delete_topic_as_ban_forever:
                     await message.reply_html(
                         "发送失败：你之前的对话已被删除。请重新发送一次当前消息。\nSend failed: Your previous conversation has been deleted. Please resend the current message."
                     )
                else:
                     # 如果是永久禁止，则发送提示给用户，并确保不重试
                     await message.reply_html(
                         "发送失败：你的对话已被永久删除。消息无法送达。\nSend failed: Your conversation has been permanently deleted. Message cannot be delivered."
                     )
                    # retry_attempt = False # 确保不重试
            else:
                 # 如果是其他类型的 BadRequest 错误，通知用户并停止重试
                 await message.reply_html(f"发送消息时遇到问题，请稍后再试。\nEncountered a problem while sending the message, please try again later.\nError: {e}")
                 retry_attempt = False # 停止重试
    except Exception as e:
        logger.error(f"Unexpected error forwarding message u2a (user: {user.id}): {e}", exc_info=True)
        await message.reply_html("发送消息时发生未知错误。\nAn unknown error occurred while sending the message.")


# 转发消息 a2u (管理员到用户)
async def forwarding_message_a2u(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 仅处理来自管理群组的消息
    if not update.message or update.message.chat.id != admin_group_id:
        return

    message = update.message
    user = update.effective_user # 发消息的管理员
    message_thread_id = message.message_thread_id

    # 1. 忽略非话题内消息 和 机器人自身的消息
    if not message_thread_id or user.is_bot:
        return

    # 2. 更新管理员信息 (可选，如果需要记录管理员信息)
    # update_user_db(user) # 如果你的 User 表也存管理员，可以取消注释

    # 3. 处理话题管理事件 (创建/关闭/重开)
    if message.forum_topic_created:
        # 理论上创建时 u2a 流程已处理，但可以加个保险或日志
        logger.info(f"Topic {message_thread_id} created event received in group.")
        f_status = db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).first()
        if not f_status:
            f = FormnStatus(message_thread_id=message_thread_id, status="opened")
            db.add(f)
            db.commit()
        elif f_status.status != "opened":
             f_status.status = "opened"
             db.add(f_status)
             db.commit()
        return # 不转发话题创建事件本身

    if message.forum_topic_closed:
        logger.info(f"Topic {message_thread_id} closed event received.")
        # 更新数据库状态
        f_status = db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).first()
        if f_status:
            f_status.status = "closed"
            db.add(f_status)
            db.commit()
        else:
            # 如果记录不存在，也创建一个标记为 closed
            f = FormnStatus(message_thread_id=message_thread_id, status="closed")
            db.add(f)
            db.commit()
        return # 不转发话题关闭事件本身

    if message.forum_topic_reopened:
        logger.info(f"Topic {message_thread_id} reopened event received.")
        # 更新数据库状态
        f_status = db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).first()
        if f_status:
            f_status.status = "opened"
            db.add(f_status)
            db.commit()
        else:
             f = FormnStatus(message_thread_id=message_thread_id, status="opened")
             db.add(f)
             db.commit()
        return # 不转发话题重开事件本身

    # 4. 查找目标用户 ID
    target_user = db.query(User).filter(User.message_thread_id == message_thread_id).first()
    if not target_user:
        logger.warning(f"Received message in topic {message_thread_id} but no user found associated with it.")
        # 可以考虑回复管理员提示此话题没有关联用户
        # await message.reply_html("错误：找不到与此话题关联的用户。", quote=True)
        return
    user_id = target_user.user_id # 目标用户 chat_id

    # 5. 检查话题是否关闭 (如果管理员在关闭的话题里发言)
    f_status = db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).first()
    if f_status and f_status.status == "closed":
        # 根据策略决定是否允许转发
        # if not allow_admin_reply_in_closed_topic: # 假设有这样一个配置
        await message.reply_html("提醒：此对话已关闭。用户的消息可能不会被发送，除非你重新打开对话。", quote=True)
        # return # 如果不允许在关闭时转发，取消下一行注释

    # 6. 准备转发参数 (主要是处理回复)
    params = {}
    if message.reply_to_message:
        reply_in_admin_group = message.reply_to_message.message_id
        # 查找这条被回复的消息在用户私聊中的对应 ID
        msg_map = db.query(MessageMap).filter(MessageMap.group_chat_message_id == reply_in_admin_group).first()
        if msg_map and msg_map.user_chat_message_id:
            params["reply_to_message_id"] = msg_map.user_chat_message_id
        else:
            logger.debug(f"Original message for reply {reply_in_admin_group} not found in user map.")

    # 7. 处理转发逻辑 (包括媒体组)
    try:
        target_chat = await context.bot.get_chat(user_id) # 获取目标用户 chat 对象

        if message.media_group_id:
             # 处理媒体组
            existing_media_group = db.query(MediaGroupMesssage).filter(
                MediaGroupMesssage.media_group_id == message.media_group_id,
                MediaGroupMesssage.chat_id == message.chat.id # chat_id 是 admin_group_id
            ).first()

            msg = MediaGroupMesssage(
                chat_id=message.chat.id,
                message_id=message.message_id,
                media_group_id=message.media_group_id,
                is_header=not existing_media_group,
                caption_html=message.caption_html if not existing_media_group else None,
            )
            db.add(msg)
            db.commit()

            if not existing_media_group:
                logger.debug(f"Received first message of media group {message.media_group_id} from admin {user.id} in topic {message_thread_id}")
                await send_media_group_later(
                    3, # 延迟3秒
                    admin_group_id, # 来源 chat_id
                    user_id,       # 目标 chat_id
                    message.media_group_id,
                    "a2u",
                    context
                )
            else:
                logger.debug(f"Received subsequent message of media group {message.media_group_id} from admin {user.id}")

        else:
            # 处理单条消息
            sent_msg = await target_chat.send_copy(
                from_chat_id=message.chat.id, # 来源是管理群组
                message_id=message.message_id,
                **params # 可能包含 reply_to_message_id
            )
            # 记录消息映射
            msg_map = MessageMap(
                group_chat_message_id=message.id,
                user_chat_message_id=sent_msg.message_id,
                user_id=user_id, # 记录是哪个用户的对话
            )
            db.add(msg_map)
            db.commit()
            logger.debug(f"Forwarded a2u: group msg({message.id}) in topic({message_thread_id}) -> user({user_id}) msg({sent_msg.message_id})")

    except BadRequest as e:
        logger.warning(f"Failed to forward message a2u (topic: {message_thread_id} -> user: {user_id}): {e}")
        # 处理用户屏蔽了机器人或删除了对话的情况
        if "bot was blocked by the user" in str(e) or "user is deactivated" in str(e) or "chat not found" in str(e).lower():
            await message.reply_html(f"⚠️ 无法将消息发送给用户 {mention_html(user_id, target_user.first_name or str(user_id))}。可能原因：用户已停用、将机器人拉黑或删除了对话。", quote=True)
            # 可以考虑在这里关闭话题或做其他处理
        else:
            await message.reply_html(f"向用户发送消息失败: {e}", quote=True)
    except Exception as e:
        logger.error(f"Unexpected error forwarding message a2u (topic: {message_thread_id} -> user: {user_id}): {e}", exc_info=True)
        await message.reply_html(f"向用户发送消息时发生未知错误: {e}", quote=True)


# --- 新增：处理用户编辑的消息 ---
async def handle_edited_user_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理来自用户私聊的已编辑消息。"""
    if not update.edited_message:
        return

    edited_msg = update.edited_message
    user = edited_msg.from_user
    edited_msg_id = edited_msg.message_id
    user_id = user.id

    logger.debug(f"处理来自用户 {user_id} 的已编辑消息 {edited_msg_id}")

    # 查找对应的群组消息
    msg_map = db.query(MessageMap).filter(MessageMap.user_chat_message_id == edited_msg_id).first()
    if not msg_map or not msg_map.group_chat_message_id:
        logger.debug(f"未找到用户编辑消息 {edited_msg_id} 在群组中的映射记录")
        return # 没有映射，无法同步

    # 查找用户的话题 ID
    u = db.query(User).filter(User.user_id == user_id).first()
    if not u or not u.message_thread_id:
        logger.debug(f"用户 {user_id} 编辑消息 {edited_msg_id} 时未找到话题 ID")
        return

    # 检查话题是否关闭 (通常编辑已不重要，但以防万一)
    f_status = db.query(FormnStatus).filter(FormnStatus.message_thread_id == u.message_thread_id).first()
    if f_status and f_status.status == "closed":
        logger.info(f"话题 {u.message_thread_id} 已关闭，忽略用户 {user_id} 的编辑同步请求。")
        return

    group_msg_id = msg_map.group_chat_message_id
    # message_thread_id = u.message_thread_id # 编辑时不需要显式传入 thread_id

    try:
        if edited_msg.text is not None: # 检查是否有文本内容 (空字符串也算)
            await context.bot.edit_message_text(
                chat_id=admin_group_id,
                message_id=group_msg_id,
                text=edited_msg.text_html, # 使用 HTML 格式
                parse_mode='HTML',
                # 不指定 reply_markup 会保留原来的按钮 (如果有)
            )
            logger.info(f"已同步用户编辑 (文本) user_msg({edited_msg_id}) 到 group_msg({group_msg_id})")
        elif edited_msg.caption is not None: # 检查是否有说明文字
             await context.bot.edit_message_caption(
                chat_id=admin_group_id,
                message_id=group_msg_id,
                caption=edited_msg.caption_html, # 使用 HTML 格式
                parse_mode='HTML',
             )
             logger.info(f"已同步用户编辑 (说明) user_msg({edited_msg_id}) 到 group_msg({group_msg_id})")
        # 暂不支持编辑媒体内容本身的同步
        else:
            logger.debug(f"用户编辑的消息 {edited_msg_id} 类型 (非文本/说明) 不支持同步。")

    except BadRequest as e:
        # 忽略 "Message is not modified" 错误，这是正常的
        if "Message is not modified" in str(e):
            logger.debug(f"同步用户编辑 user_msg({edited_msg_id}) 到 group_msg({group_msg_id}) 时消息无变化。")
        else:
            logger.warning(f"同步用户编辑 user_msg({edited_msg_id}) 到 group_msg({group_msg_id}) 失败: {e}")
    except Exception as e:
        logger.error(f"同步用户编辑 user_msg({edited_msg_id}) 到 group_msg({group_msg_id}) 时发生意外错误: {e}", exc_info=True)


# --- 新增：处理管理员编辑的消息 ---
async def handle_edited_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理来自管理群组话题的已编辑消息。"""
    if not update.edited_message or update.edited_message.chat.id != admin_group_id:
        return

    edited_msg = update.edited_message
    edited_msg_id = edited_msg.message_id
    message_thread_id = edited_msg.message_thread_id

    # 忽略非话题内或机器人自身的消息编辑
    if not message_thread_id or edited_msg.from_user.is_bot:
        return

    logger.debug(f"处理来自管理群组话题 {message_thread_id} 的已编辑消息 {edited_msg_id}")

    # 查找对应的用户私聊消息
    msg_map = db.query(MessageMap).filter(MessageMap.group_chat_message_id == edited_msg_id).first()
    if not msg_map or not msg_map.user_chat_message_id:
        logger.debug(f"未找到管理员编辑消息 {edited_msg_id} 在用户私聊中的映射记录")
        return

    user_chat_msg_id = msg_map.user_chat_message_id
    user_id = msg_map.user_id # 从映射记录获取目标用户 ID

    # 检查话题状态 (可选，管理员可能希望编辑关闭话题中的消息)
    # f_status = db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).first()
    # if f_status and f_status.status == "closed":
    #     logger.info(f"Topic {message_thread_id} is closed. Skipping admin edit sync.")
    #     # await edited_msg.reply_html("提醒：话题已关闭，编辑可能不会同步给用户。", quote=True)
    #     return

    try:
        if edited_msg.text is not None:
            await context.bot.edit_message_text(
                chat_id=user_id,
                message_id=user_chat_msg_id,
                text=edited_msg.text_html,
                parse_mode='HTML',
            )
            logger.info(f"已同步管理员编辑 (文本) group_msg({edited_msg_id}) 到 user_msg({user_chat_msg_id})")
        elif edited_msg.caption is not None:
             await context.bot.edit_message_caption(
                chat_id=user_id,
                message_id=user_chat_msg_id,
                caption=edited_msg.caption_html,
                parse_mode='HTML',
             )
             logger.info(f"已同步管理员编辑 (说明) group_msg({edited_msg_id}) 到 user_msg({user_chat_msg_id})")
        else:
             logger.debug(f"管理员编辑的消息 {edited_msg_id} 类型 (非文本/说明) 不支持同步。")

    except BadRequest as e:
        if "Message is not modified" in str(e):
             logger.debug(f"同步管理员编辑 group_msg({edited_msg_id}) 到 user_msg({user_chat_msg_id}) 时消息无变化。")
        elif "bot was blocked by the user" in str(e) or "user is deactivated" in str(e) or "chat not found" in str(e).lower():
             logger.warning(f"同步管理员编辑 group_msg({edited_msg_id}) 到 user_msg({user_chat_msg_id}) 失败: 用户可能已拉黑或停用。")
             # 可以考虑通知管理员
             # await edited_msg.reply_html(f"⚠️ 无法向用户 {user_id} 同步编辑：用户可能已拉黑或停用。", quote=True)
        else:
             logger.warning(f"同步管理员编辑 group_msg({edited_msg_id}) 到 user_msg({user_chat_msg_id}) 失败: {e}")
    except Exception as e:
        logger.error(f"同步管理员编辑 group_msg({edited_msg_id}) 到 user_msg({user_chat_msg_id}) 时发生意外错误: {e}", exc_info=True)


# 清理话题 (clear 命令)
async def clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message

    # 权限检查
    if user.id not in admin_user_ids:
        await message.reply_html("你没有权限执行此操作。")
        return

    # 检查是否在话题内
    message_thread_id = message.message_thread_id
    if not message_thread_id:
        await message.reply_html("请在需要清除的用户对话（话题）中执行此命令。")
        return

    # 查找关联的用户
    target_user = db.query(User).filter(User.message_thread_id == message_thread_id).first()

    try:
        # 删除话题
        await context.bot.delete_forum_topic(
            chat_id=admin_group_id,
            message_thread_id=message_thread_id
        )
        logger.info(f"Admin {user.id} cleared topic {message_thread_id}")

        # 从数据库移除话题状态和用户关联
        db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).delete()
        if target_user:
            target_user.message_thread_id = None
            db.add(target_user)
        # 提交更改
        db.commit()
        # 可选：发送一个确认消息到 General (如果 General 可用)
        # await context.bot.send_message(admin_group_id, f"管理员 {mention_html(user.id, user.full_name)} 清除了话题 {message_thread_id}", parse_mode='HTML')

    except BadRequest as e:
        logger.error(f"Failed to delete topic {message_thread_id} by admin {user.id}: {e}")
        await message.reply_html(f"清除话题失败: {e}", quote=True)
        # 即便删除失败，也尝试清理数据库关联
        db.query(FormnStatus).filter(FormnStatus.message_thread_id == message_thread_id).delete()
        if target_user:
            target_user.message_thread_id = None
            db.add(target_user)
        db.commit()
    except Exception as e:
         logger.error(f"Unexpected error clearing topic {message_thread_id} by admin {user.id}: {e}", exc_info=True)
         await message.reply_html(f"清除话题时发生意外错误: {e}", quote=True)

    # --- 用户消息删除逻辑 ---
    if is_delete_user_messages and target_user:
        logger.info(f"Attempting to delete messages for user {target_user.user_id} related to cleared topic {message_thread_id}")
        # 查找该用户所有映射过的消息
        all_user_messages_map = db.query(MessageMap).filter(MessageMap.user_id == target_user.user_id).all()
        user_message_ids_to_delete = [msg.user_chat_message_id for msg in all_user_messages_map if msg.user_chat_message_id]

        if user_message_ids_to_delete:
            deleted_count = 0
            batch_size = 100 # Telegram 一次最多删除 100 条
            for i in range(0, len(user_message_ids_to_delete), batch_size):
                batch = user_message_ids_to_delete[i:i + batch_size]
                try:
                    success = await context.bot.delete_messages(
                        chat_id=target_user.user_id,
                        message_ids=batch
                    )
                    if success:
                        deleted_count += len(batch)
                    else:
                        logger.warning(f"Failed to delete a batch of messages for user {target_user.user_id}.")
                        # 可以尝试逐条删除作为后备，但会很慢
                except BadRequest as e:
                     logger.warning(f"Error deleting messages batch for user {target_user.user_id}: {e}")
                     # 如果是 "Message ids must be unique"，说明列表有重复，需要去重
                     # 如果是 "Message can't be deleted"，可能是消息太旧或权限问题
                except Exception as e:
                     logger.error(f"Unexpected error deleting messages for user {target_user.user_id}: {e}", exc_info=True)

            logger.info(f"Deleted {deleted_count} out of {len(user_message_ids_to_delete)} messages for user {target_user.user_id}.")
            # 清除该用户的所有消息映射记录
            db.query(MessageMap).filter(MessageMap.user_id == target_user.user_id).delete()
            db.commit()
            logger.info(f"Cleared message map entries for user {target_user.user_id}.")


# 广播回调 (保持不变)
async def _broadcast(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    if not isinstance(job_data, str) or "_" not in job_data:
        logger.error(f"Invalid job data format for broadcast: {job_data}")
        return

    try:
        msg_id, chat_id = job_data.split("_", 1)
        msg_id = int(msg_id)
        chat_id = int(chat_id)
    except ValueError:
        logger.error(f"Could not parse msg_id and chat_id from broadcast job data: {job_data}")
        return

    users = db.query(User).filter(User.message_thread_id != None).all() # 只广播给活跃用户? 或 all()?
    logger.info(f"Starting broadcast of message {msg_id} from chat {chat_id} to {len(users)} users.")
    success = 0
    failed = 0
    block_or_deactivated = 0

    for u in users:
        try:
            # 使用 copy_message 更灵活，允许添加按钮等
            await context.bot.copy_message(
                chat_id=u.user_id,
                from_chat_id=chat_id,
                message_id=msg_id
            )
            success += 1
            await asyncio.sleep(0.1) # 稍作延迟防止触发 Flood Limits
        except BadRequest as e:
            if "bot was blocked by the user" in str(e) or "user is deactivated" in str(e):
                block_or_deactivated += 1
                logger.debug(f"Broadcast failed to user {u.user_id}: Blocked or deactivated.")
                # 可选：将这些用户标记为非活跃
            else:
                failed += 1
                logger.warning(f"Broadcast failed to user {u.user_id}: {e}")
        except Exception as e:
            failed += 1
            logger.error(f"Unexpected error broadcasting to user {u.user_id}: {e}", exc_info=True)

    logger.info(f"Broadcast finished. Success: {success}, Failed: {failed}, Blocked/Deactivated: {block_or_deactivated}")
    # 可以考虑通知发起广播的管理员结果
    # originator_admin_id = context.job.context.get('admin_id') # 需要在 run_once 时传入
    # if originator_admin_id:
    #     await context.bot.send_message(originator_admin_id, f"广播完成：成功 {success}，失败 {failed}，屏蔽/停用 {block_or_deactivated}")


# 广播命令 (保持不变)
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if user.id not in admin_user_ids:
        await update.message.reply_html("你没有权限执行此操作。")
        return

    if not update.message.reply_to_message:
        await update.message.reply_html(
            "请回复一条你想要广播的消息来使用此命令。"
        )
        return

    broadcast_message = update.message.reply_to_message
    job_data = f"{broadcast_message.id}_{broadcast_message.chat.id}"

    context.job_queue.run_once(
        _broadcast,
        when=timedelta(seconds=1), # 延迟1秒开始执行
        data=job_data,
        name=f"broadcast_{broadcast_message.id}"
        # context={"admin_id": user.id} # 如果需要在 _broadcast 回调中知道是谁发起的
    )
    await update.message.reply_html(f"📢 广播任务已计划执行。将广播消息 ID: {broadcast_message.id}")


# 错误处理 (保持不变)
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """记录错误日志。"""
    logger.error(f"处理更新时发生异常: {context.error}", exc_info=context.error)
    # 对于特定类型的常见错误，可以添加更详细的处理或用户提示
    # 例如： 处理用户在私聊中发送命令（如果未定义）
    # if isinstance(context.error, CommandInvalid) and isinstance(update, Update) and update.message and update.message.chat.type == ChatType.PRIVATE:
    #     await update.message.reply_text("未知命令。直接发送消息即可与客服沟通。")


# --- Main Execution ---
if __name__ == "__main__":
    # 使用基于文件的持久化存储用户和聊天数据
    pickle_persistence = PicklePersistence(filepath=f"./assets/{app_name}.pickle")

    application = (
        ApplicationBuilder()
        .token(bot_token)
        .persistence(persistence=pickle_persistence)
        # .concurrent_updates(True) # 可以考虑开启并发处理更新
        .build()
    )

    # --- 命令处理器 ---
    application.add_handler(CommandHandler("start", start, filters.ChatType.PRIVATE))
    application.add_handler(CommandHandler("clear", clear, filters.Chat(admin_group_id) & filters.REPLY)) # clear 需要在话题内回复才能执行
    application.add_handler(CommandHandler("broadcast", broadcast, filters.Chat(admin_group_id) & filters.REPLY)) # broadcast 需要回复

    # --- 消息处理器 ---
    # 1. 用户发送 *新* 消息给机器人 (私聊)
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & ~filters.COMMAND & ~filters.UpdateType.EDITED_MESSAGE,
            forwarding_message_u2a
        )
    )
    # 2. 管理员在话题中发送 *新* 消息 (管理群组)
    application.add_handler(
        MessageHandler(
            filters.Chat(admin_group_id) & filters.IS_TOPIC_MESSAGE & ~filters.COMMAND & ~filters.UpdateType.EDITED_MESSAGE, # 确保是话题内消息
            forwarding_message_a2u
        )
    )
    # 3. 用户 *编辑* 发给机器人的消息 (私聊)
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.UpdateType.EDITED_MESSAGE,
            handle_edited_user_message
        )
    )
    # 4. 管理员 *编辑* 话题中的消息 (管理群组)
    application.add_handler(
        MessageHandler(
            filters.Chat(admin_group_id) & filters.IS_TOPIC_MESSAGE & filters.UpdateType.EDITED_MESSAGE, # 确保是话题内编辑
            handle_edited_admin_message
        )
    )

    # --- 回调查询处理器 ---
    application.add_handler(
        CallbackQueryHandler(callback_query_vcode, pattern="^vcode_")
    )

    # --- 错误处理器 ---
    application.add_error_handler(error_handler)

    # --- 启动 Bot ---
    logger.info("Bot starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES) # 接收所有类型的更新
