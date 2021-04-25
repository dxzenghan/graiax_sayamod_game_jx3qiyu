import os
from datetime import datetime

from pony.orm import *


db = Database()


class QiYu(db.Entity):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    trigger_probability = Required(float)  # 触发概率
    cooling_period = Required(int)  # 冷却时间（单位：小时）
    notice_template = Required(str)  # 触发通知模板
    trigger_record = Set("QiyuTriggerRecord")  # one2many record


class QiyuTriggerRecord(db.Entity):
    qiyu = Optional(QiYu)  # 奇遇 id
    group_id = Required(str)  # 群号
    member_id = Required(str)  # 群成员 QQ 号
    last_trigger_datetime = Required(datetime, precision=6)  # 上一次触发时间


# 每个 orm 文件必须要有以下两行代码
db.bind(
    provider="sqlite",
    filename=os.path.join(os.environ.get("QIYU_STATIC_FILEPATH"), "db", "qiyu.sqlite"),
    create_db=True,
)
db.generate_mapping(create_tables=True)