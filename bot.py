import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
from functools import lru_cache
import pandas as pd
import redis
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

# --- КОНФІГУРАЦІЯ ---
TOKEN = "7848693835:AAFYauCb5vU-VZfbIa1uLEOCYSepz9QZO0E"
FILE_NAME = "rozklad_pro.xlsx"
REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# --- РОЗКЛАД ДЗВІНКІВ ---
BELL_SCHEDULE = [
    {"num": 1, "start": "08:30", "end": "09:50", "break": "10 хв"},
    {"num": 2, "start": "10:00", "end": "11:20", "break": "25 хв (Велика)"},
    {"num": 3, "start": "11:45", "end": "13:05", "break": "10 хв"},
    {"num": 4, "start": "13:15", "end": "14:35", "break": "10 хв"},
    {"num": 5, "start": "14:45", "end": "16:05", "break": "10 хв"},
    {"num": 6, "start": "16:15", "end": "17:35", "break": "10 хв"},
    {"num": 7, "start": "17:45", "end": "19:05", "break": "-"},
]

# True = Парні тижні це Чисельник
INVERT_WEEK_LOGIC = True 

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
bot = Bot(token=TOKEN)
dp = Dispatcher()
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

class Form(StatesGroup):
    choosing_groups = State()

# --- ЛОГІКА ТИЖНІВ ---
def get_week_status(date_obj=None):
    if date_obj is None: date_obj = datetime.now()
    week_num = date_obj.isocalendar()[1]
    is_even = (week_num % 2 == 0)
    if INVERT_WEEK_LOGIC:
        return "numerator" if is_even else "denominator"
    else:
        return "denominator" if is_even else "numerator"

def get_week_ua(w_type):
    return "🟥 Чисельник" if w_type == "numerator" else "🟦 Знаменник"

# --- РОБОТА З EXCEL ---
@lru_cache(maxsize=1)
def load_schedule_cached():
    try:
        return pd.read_excel(FILE_NAME, dtype=str)
    except Exception as e:
        logging.error(f"Error loading Excel: {e}")
        return pd.DataFrame()

def clear_cache():
    load_schedule_cached.cache_clear()

def get_all_teachers():
    df = load_schedule_cached()
    if df.empty: return []
    teachers = set()
    raw_list = df['Викладач'].dropna().unique()
    for item in raw_list:
        if str(item).strip() in ["-", "nan", ""]: continue
        parts = str(item).split("//")
        for p in parts:
            name = p.strip()
            if len(name) > 2: teachers.add(name)
    return sorted(list(teachers))

# --- ФОРМАТУВАННЯ ---
def format_lesson_entry_for_week(subject, teacher, room, w_current, group):
    if pd.isna(subject) or str(subject) in ["-", "nan"]: return None
    subject = str(subject)
    teacher = str(teacher)
    room = str(room) if str(room) not in ["-", "nan"] else ""
    
    # Додаємо номер групи для ясності
    grp_str = f" <i>(Гр. {group})</i>"

    if "//" in subject:
        parts_s = subject.split("//")
        parts_t = teacher.split("//") if "//" in teacher else [teacher, teacher]
        s1, s2 = parts_s[0].strip(), parts_s[1].strip() if len(parts_s) > 1 else ""
        t1, t2 = parts_t[0].strip(), parts_t[1].strip() if len(parts_t) > 1 else parts_t[0]
        return (f"🔄 <b>Мигалка:</b>\n"
                f"   🟥 {s1} ({t1}){grp_str}\n"
                f"   🟦 {s2} ({t2}){grp_str}")
                
    if "(ч)" in subject:
        return f"🟥 <b>(Чис):</b> {subject.replace('(ч)','').strip()} ({teacher}){grp_str}"
    if "(з)" in subject:
        return f"🟦 <b>(Знам):</b> {subject.replace('(з)','').strip()} ({teacher}){grp_str}"
        
    return f"▫️ {subject} ({teacher}){grp_str}"

def filter_lesson_current(subject, w_type):
    if pd.isna(subject) or str(subject) in ["-", "nan"]: return None
    subject = str(subject)
    
    if "//" in subject:
        parts = subject.split("//")
        return parts[0].strip() if w_type == "numerator" and len(parts) > 0 else parts[1].strip() if len(parts) > 1 else parts[0].strip()
    
    if "(ч)" in subject: return subject.replace("(ч)", "").strip() if w_type == "numerator" else None
    if "(з)" in subject: return subject.replace("(з)", "").strip() if w_type == "denominator" else None
        
    return subject

def get_schedule_filtered(user_id, day=None, specific_time=None):
    role = r.get(f"user:{user_id}:role")
    df = load_schedule_cached()
    if df.empty: return pd.DataFrame()
    if day: df = df[df['День'] == day]
    if specific_time: df = df[df['Час'] == specific_time]

    if role == "student":
        groups = r.smembers(f"user:{user_id}:groups")
        if not groups: return pd.DataFrame()
        return df[df['Група'].isin(groups)]
    elif role == "teacher":
        t_name = r.get(f"user:{user_id}:teacher_name")
        if not t_name: return pd.DataFrame()
        return df[df['Викладач'].str.contains(t_name, na=False, regex=False)]
    return pd.DataFrame()

# --- КЛАВІАТУРИ ---
def kb_start_roles():
    b = InlineKeyboardBuilder()
    b.button(text="🎓 Я Студент", callback_data="role_student")
    b.button(text="💼 Я Викладач", callback_data="role_teacher")
    b.adjust(1)
    return b.as_markup()

def kb_courses():
    df = load_schedule_cached()
    courses = sorted(df['Курс'].dropna().unique(), key=lambda x: int(x) if str(x).isdigit() else 0)
    b = InlineKeyboardBuilder()
    for c in courses: b.button(text=f"{c} курс", callback_data=f"course_{c}")
    b.adjust(2)
    return b.as_markup()

def kb_groups_multiselect(course, selected_groups):
    df = load_schedule_cached()
    groups = sorted(df[df['Курс'] == str(course)]['Група'].dropna().unique())
    b = InlineKeyboardBuilder()
    for g in groups:
        text = f"✅ {g}" if g in selected_groups else g
        b.button(text=text, callback_data=f"toggle_group_{g}")
    b.adjust(3)
    b.row(types.InlineKeyboardButton(text="💾 Зберегти", callback_data="save_groups"))
    return b.as_markup()

def kb_teachers_select():
    teachers = get_all_teachers()
    b = InlineKeyboardBuilder()
    for t in teachers[:60]: b.button(text=t, callback_data=f"set_teacher_{t}")
    b.adjust(2)
    return b.as_markup()

def kb_main_menu():
    b = ReplyKeyboardBuilder()
    b.button(text="🔴 Яка зараз пара?")
    b.button(text="🔔 Дзвінки")
    b.button(text="📅 Розклад на сьогодні")
    b.button(text="🗓 Розклад на тиждень")
    b.button(text="⚙️ Налаштування")
    b.adjust(1, 1, 2, 1)
    return b.as_markup(resize_keyboard=True)

# --- HANDLERS ---
@dp.message(Command("start"))
async def cmd_start(msg: types.Message, state: FSMContext):
    await state.clear()
    await msg.answer("👋 <b>Вітаю!</b>\nОберіть вашу роль:", parse_mode="HTML", reply_markup=kb_start_roles())

@dp.callback_query(F.data == "role_student")
async def role_student(cb: types.CallbackQuery):
    r.set(f"user:{cb.from_user.id}:role", "student")
    await cb.message.edit_text("🎓 Оберіть курс:", reply_markup=kb_courses())

@dp.callback_query(F.data == "role_teacher")
async def role_teacher(cb: types.CallbackQuery):
    r.set(f"user:{cb.from_user.id}:role", "teacher")
    await cb.message.edit_text("💼 Оберіть себе:", reply_markup=kb_teachers_select())

@dp.callback_query(F.data.startswith("course_"))
async def course_chosen(cb: types.CallbackQuery, state: FSMContext):
    course = cb.data.split("_")[1]
    await state.update_data(current_course=course)
    r.delete(f"user:{cb.from_user.id}:groups")
    await cb.message.edit_text(f"✅ {course} курс. Оберіть групи:", reply_markup=kb_groups_multiselect(course, []))
    await state.set_state(Form.choosing_groups)

@dp.callback_query(F.data.startswith("toggle_group_"))
async def toggle_group(cb: types.CallbackQuery, state: FSMContext):
    grp = cb.data.split("_")[2]
    uid = cb.from_user.id
    key = f"user:{uid}:groups"
    if r.sismember(key, grp): r.srem(key, grp)
    else: r.sadd(key, grp)
    data = await state.get_data()
    try: await cb.message.edit_reply_markup(reply_markup=kb_groups_multiselect(data.get("current_course"), r.smembers(key)))
    except: pass

@dp.callback_query(F.data == "save_groups")
async def save_groups(cb: types.CallbackQuery, state: FSMContext):
    if not r.smembers(f"user:{cb.from_user.id}:groups"): return await cb.answer("Оберіть групу!", show_alert=True)
    await cb.message.delete()
    await cb.message.answer("✅ Налаштовано!", reply_markup=kb_main_menu())
    await state.clear()

@dp.callback_query(F.data.startswith("set_teacher_"))
async def set_teacher(cb: types.CallbackQuery):
    t_name = cb.data.split("_", 2)[2]
    r.set(f"user:{cb.from_user.id}:teacher_name", t_name)
    await cb.message.delete()
    await cb.message.answer(f"✅ Вітаю, {t_name}!", reply_markup=kb_main_menu())

@dp.message(F.text == "⚙️ Налаштування")
async def settings(msg: types.Message, state: FSMContext):
    await cmd_start(msg, state)

@dp.message(F.text == "🔔 Дзвінки")
async def show_bells(msg: types.Message):
    text = "🔔 <b>РОЗКЛАД ДЗВІНКІВ</b>\n\n"
    for item in BELL_SCHEDULE:
        text += (f"<b>{item['num']} пара:</b> {item['start']} — {item['end']}\n"
                 f"☕ <i>Перерва: {item['break']}</i>\n\n")
    await msg.answer(text, parse_mode="HTML")

# --- ОСНОВНІ ФУНКЦІЇ ---
def is_lesson_active(start_time_str):
    try:
        now = datetime.now()
        end_time_str = None
        for item in BELL_SCHEDULE:
            if item['start'] == start_time_str:
                end_time_str = item['end']
                break
        start = datetime.strptime(start_time_str, "%H:%M").replace(year=now.year, month=now.month, day=now.day)
        if end_time_str:
            end = datetime.strptime(end_time_str, "%H:%M").replace(year=now.year, month=now.month, day=now.day)
        else:
            end = start + timedelta(minutes=80) 
        return start <= now <= end
    except: return False

@dp.message(F.text == "🔴 Яка зараз пара?")
async def current_lesson(msg: types.Message):
    user_id = msg.from_user.id
    today = datetime.now().strftime("%A")
    w_type = get_week_status()
    w_label = get_week_ua(w_type)
    
    df = get_schedule_filtered(user_id, day=today)
    found_messages = [] 
    
    for _, row in df.iterrows():
        subj = filter_lesson_current(row['Предмет'], w_type)
        if subj and is_lesson_active(row['Час']):
            role = r.get(f"user:{user_id}:role")
            info = f"Група: {row['Група']}" if role == "teacher" else f"👨‍🏫 {row['Викладач']}"
            txt = f"🔥 <b>ЗАРАЗ ({w_label}):</b>\n📚 {subj}\n⏰ {row['Час']}\n{info}"
            if str(row['Кабінет/Zoom']) not in ['-', 'nan']: txt += f"\n🔗 {row['Кабінет/Zoom']}"
            found_messages.append(txt)
            
    if found_messages:
        await msg.answer("\n\n➖ ➖ ➖\n\n".join(found_messages), parse_mode="HTML", disable_web_page_preview=True)
    else: 
        await msg.answer(f"☕ Зараз пар немає ({w_label}). Гляньте \"🔔 Дзвінки\".")

@dp.message(F.text == "📅 Розклад на сьогодні")
async def show_today(msg: types.Message):
    user_id = msg.from_user.id
    day = datetime.now().strftime("%A")
    w_type = get_week_status()
    w_label = get_week_ua(w_type)
    df = get_schedule_filtered(user_id, day=day).sort_values("Час")
    
    text = f"📅 <b>СЬОГОДНІ</b> ({w_label})\n"
    has_data = False
    for _, row in df.iterrows():
        subj = filter_lesson_current(row['Предмет'], w_type)
        if subj:
            has_data = True
            role = r.get(f"user:{user_id}:role")
            info = row['Група'] if role == "teacher" else row['Викладач']
            text += f"\n⏰ {row['Час']} — <b>{subj}</b>\n   <i>{info}</i>"
    
    if not has_data: text += "\nПар немає! 🎉"
    await msg.answer(text, parse_mode="HTML")

@dp.message(F.text == "🗓 Розклад на тиждень")
async def show_week(msg: types.Message):
    user_id = msg.from_user.id
    w_type = get_week_status()
    w_label = get_week_ua(w_type)
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    days_ua = {'Monday': 'ПН', 'Tuesday': 'ВТ', 'Wednesday': 'СР', 'Thursday': 'ЧТ', 'Friday': 'ПТ', 'Saturday': 'СБ'}
    
    full_text = f"🗓 <b>РОЗКЛАД НА ТИЖДЕНЬ</b>\n📌 {w_label}\n"
    df_all = get_schedule_filtered(user_id)
    if df_all.empty: return await msg.answer("Розклад порожній.")

    for day in days_order:
        day_df = df_all[df_all['День'] == day].sort_values("Час")
        if day_df.empty: continue
        day_lessons = []
        for _, row in day_df.iterrows():
            # Тут ми передаємо row['Група'], щоб бот міг її показати
            entry = format_lesson_entry_for_week(row['Предмет'], row['Викладач'], row['Кабінет/Zoom'], w_type, row['Група'])
            if entry: day_lessons.append(f"⏰ <b>{row['Час']}</b>\n{entry}")
        if day_lessons: full_text += f"\n🔰 <b>{days_ua[day]}</b>:\n" + "\n".join(day_lessons) + "\n"

    if len(full_text) > 4000:
        parts = []
        while len(full_text) > 0:
            if len(full_text) > 4000:
                split_pos = full_text[:4000].rfind('\n')
                if split_pos == -1: split_pos = 4000
                parts.append(full_text[:split_pos])
                full_text = full_text[split_pos:]
            else:
                parts.append(full_text)
                break
        for part in parts:
            await msg.answer(part, parse_mode="HTML")
    else:
        await msg.answer(full_text, parse_mode="HTML")

# --- SCHEDULER (ОПОВІЩЕННЯ) ---
async def scheduler():
    logging.info("Scheduler started...")
    while True:
        now = datetime.now()
        if now.second == 0:
            w_type = get_week_status(now)
            next_min_stud = (now + timedelta(minutes=1)).strftime("%H:%M")
            next_min_teach = (now + timedelta(minutes=5)).strftime("%H:%M")
            day = now.strftime("%A")
            clear_cache()
            
            alerts_queue = {} 

            for key in r.keys("user:*:role"):
                uid = key.split(":")[1]
                role = r.get(f"user:{uid}:role")
                check_time = next_min_teach if role == "teacher" else next_min_stud
                
                df = get_schedule_filtered(uid, day=day, specific_time=check_time)
                
                for _, row in df.iterrows():
                    subj = filter_lesson_current(row['Предмет'], w_type)
                    if subj:
                        warn = "5 хвилин" if role == "teacher" else "1 хвилину"
                        link = str(row['Кабінет/Zoom'])
                        link_html = f"\n🔗 <a href='{link}'>ВХІД</a>" if link.lower() not in ['-', 'nan'] else f"\n🚪 {link}"
                        
                        info_line = f"Група: {row['Група']}" if role == "teacher" else f"👨‍🏫 {row['Викладач']}"

                        msg_text = (f"🔔 <b>Через {warn}!</b>\n"
                                    f"📚 {subj}\n"
                                    f"<i>{info_line}</i>"
                                    f"{link_html}")
                        
                        if uid not in alerts_queue: alerts_queue[uid] = []
                        alerts_queue[uid].append(msg_text)
            
            for uid, messages in alerts_queue.items():
                try:
                    final_text = "\n\n➖➖➖➖➖➖\n\n".join(messages)
                    await bot.send_message(uid, final_text, parse_mode="HTML", disable_web_page_preview=True)
                except: pass

            await asyncio.sleep(60)
        else:
            await asyncio.sleep(1)

async def main():
    print("Bot started...")
    asyncio.create_task(scheduler())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
