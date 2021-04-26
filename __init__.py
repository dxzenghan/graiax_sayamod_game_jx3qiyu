import arrow
import asyncio
from asyncio import AbstractEventLoop
import concurrent.futures
import json
import os
import random
from threading import Thread
import time

from typing import Iterable, List
from graia.application import GraiaMiraiApplication

from graia.application.event.messages import GroupMessage
from graia.application.message.chain import MessageChain
from graia.application.message.parser.kanata import Kanata
from graia.application.message.elements.internal import At, Plain, Source
from graia.application.group import Group, Member
from graia.application.message.parser.signature import FullMatch, RequireParam
from graia.broadcast.exceptions import ExecutionStop
from graia.saya import Saya, Channel
from graia.saya.event import SayaModuleInstalled
from graia.saya.builtins.broadcast.schema import ListenerSchema
from graia.broadcast import Broadcast
from graia.scheduler import GraiaScheduler
from graia.scheduler.timers import *

from pony.orm import *
from datetime import datetime

from .builtins.loop_manager import LoopManager


saya = Saya.current()
channel = Channel.current()
loop_manager = LoopManager()


@channel.use(ListenerSchema(listening_events=[SayaModuleInstalled]))
def initialize():
    # 创建模块文件夹
    os.environ.update(
        {
            "QIYU_STATIC_FILEPATH": os.path.join(
                os.getcwd(), "data", "graiax_sayamod_game_jx3qiyu"
            )
        }
    )

    if not os.path.exists(
        db_filepath := os.path.join(os.environ.get("QIYU_STATIC_FILEPATH"), "db")
    ):
        os.makedirs(db_filepath)

    loop_manager.new_event_loop("QiyuThread")

    from . import tables

    @db_session
    def load_qiyu_trigger_probability():
        # 读取奇遇概率数据到内存中
        os.environ.update(
            {
                "QIYU_TRIGGER_PROBABILITY": json.dumps(
                    {
                        item.name: item.trigger_probability
                        for item in select(q for q in tables.QiYu)[:]
                    }
                )
            }
        )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        saya.broadcast.loop.run_in_executor(executor, load_qiyu_trigger_probability)


@channel.use(ListenerSchema(listening_events=[GroupMessage]))
async def trigger(
    app: GraiaMiraiApplication, group: Group, member: Member, messageChain: MessageChain
):
    from . import tables

    # 查询在冷却期内的奇遇
    @db_session
    def query_cooled_qiyu():
        cooling_qiyu = [
            r.qiyu
            for r in select(_ for _ in tables.QiyuTriggerRecord)
            if (r.member_id == member.id)  # 自己已经出过的奇遇
            or (
                ((datetime.now() - r.last_trigger_datetime).seconds / 3600)
                < r.qiyu.cooling_period
            )  # 别人出过的奇遇且还在冷却时间中
        ]
        return [
            _
            for _ in select(q for q in tables.QiYu).filter(
                lambda q: q not in cooling_qiyu
            )
        ]

    # 随机抽取一个在冷却期内的奇遇，根据概率判定是否触发
    def trigger_qiyu(cooled_qiyu: List[tables.QiYu]) -> str:
        if cooled_qiyu == []:
            return None
        select_qiyu = random.sample(cooled_qiyu, k=1)[0]
        if random.randint(select_qiyu.trigger_probability * 100, 100) == 100:
            return select_qiyu
        else:
            return None

    # 通知触发奇遇
    async def send_trigger_notice(
        app: GraiaMiraiApplication,
        group: Group,
        member: Member,
        messageChain: MessageChain,
        triggered_qiyu: tables.QiYu,
    ):
        await app.sendGroupMessage(
            group,
            messageChain.create(
                [
                    Plain(
                        triggered_qiyu.notice_template.format(
                            f"{member.name}({member.id})"
                        )
                    )
                ]
            ),
        )

    # 奇遇触发记录
    @db_session
    def record_trigger(
        group: Group,
        member: Member,
        messageChain: MessageChain,
        triggerd_qiyu: tables.QiYu,
    ):
        tables.QiyuTriggerRecord(
            qiyu=triggerd_qiyu.id,
            group_id=str(group.id),
            member_id=str(member.id),
            last_trigger_datetime=arrow.get(messageChain.getFirst(Source).time)
            .to("local")
            .format("YYYY-MM-DD HH:mm:ss"),
        )
        commit()

    async def task(app, group, member, messageChain):
        cooled_qiyu = query_cooled_qiyu()
        triggered_qiyu = trigger_qiyu(cooled_qiyu)
        if (triggered_qiyu is not None) and (group.id == 712290595):
            loop_manager.create_task(
                send_trigger_notice(app, group, member, messageChain, triggered_qiyu),
                "MainThread",
            )
            record_trigger(group, member, messageChain, triggered_qiyu)

    loop_manager.create_task(task(app, group, member, messageChain), "QiyuThread")


@channel.use(
    ListenerSchema(
        listening_events=[GroupMessage],
        inline_dispatchers=[Kanata([FullMatch("查询奇遇："), RequireParam(name="qiyu")])],
    )
)
async def query_single(
    app: GraiaMiraiApplication,
    group: Group,
    member: Member,
    messageChain: MessageChain,
    qiyu: MessageChain,
):
    from . import tables

    # 查询个人奇遇触发记录
    @db_session
    def query_member_qiyu():
        return (
            r.qiyu.name
            for r in select(r for r in tables.QiyuTriggerRecord).filter(
                lambda r: (r.group_id == str(group.id))
                and (r.member_id == str(member.id))
            )
        )

    # 生成个人奇遇信息
    @db_session
    def generate_member_qiyu_trigger_info(member_qiyu: Iterable):
        member_qiyu_trigger_info = {
            qiyu: False
            for qiyu in json.loads(os.environ.get("QIYU_TRIGGER_PROBABILITY"))
        }
        for qiyu in member_qiyu:
            member_qiyu_trigger_info.update({qiyu: True})
        return member_qiyu_trigger_info

    # 发送查询结果
    async def send_member_qiyu_trigger_info(
        app: GraiaMiraiApplication,
        group: Group,
        member: Member,
        messageChain: MessageChain,
        qiyu: MessageChain,
        member_qiyu_trigger_info: dict,
    ):
        await app.sendGroupMessage(
            group,
            messageChain.create(
                [
                    Plain(
                        f"{member.name}({member.id}) 侠士的奇遇查询结果如下({sum(member_qiyu_trigger_info.values())}/{len(member_qiyu_trigger_info)}):\n"
                    ),
                    Plain(
                        f"{qiyu.asDisplay()}: {'已触发' if member_qiyu_trigger_info[qiyu.asDisplay()] else '未触发'}"
                    ),
                ]
            ),
        )

    async def task(app, group, member, messageChain, qiyu):
        if qiyu is None:
            raise ExecutionStop
        member_qiyu = query_member_qiyu()
        qiyu_trigger_info = generate_member_qiyu_trigger_info(member_qiyu)
        loop_manager.create_task(
            send_member_qiyu_trigger_info(
                app, group, member, messageChain, qiyu, qiyu_trigger_info
            ),
            "MainThread",
        )

    loop_manager.create_task(task(app, group, member, messageChain, qiyu), "QiyuThread")


@channel.use(
    ListenerSchema(
        listening_events=[GroupMessage],
        inline_dispatchers=[Kanata([FullMatch("查询奇遇")])],
    )
)
async def query_all(
    app: GraiaMiraiApplication, group: Group, member: Member, messageChain: MessageChain
):
    from . import tables

    # 查询个人奇遇触发记录
    @db_session
    def query_member_qiyu():
        return (
            r.qiyu.name
            for r in select(r for r in tables.QiyuTriggerRecord).filter(
                lambda r: (r.group_id == str(group.id))
                and (r.member_id == str(member.id))
            )
        )

    # 生成个人奇遇信息
    @db_session
    def generate_member_qiyu_trigger_info(member_qiyu: Iterable):
        member_qiyu_trigger_info = {
            qiyu: False
            for qiyu in json.loads(os.environ.get("QIYU_TRIGGER_PROBABILITY"))
        }
        for qiyu in member_qiyu:
            member_qiyu_trigger_info.update({qiyu: True})
        return member_qiyu_trigger_info

    # 发送查询结果
    async def send_member_qiyu_trigger_info(
        app: GraiaMiraiApplication,
        group: Group,
        member: Member,
        messageChain: MessageChain,
        member_qiyu_trigger_info: dict,
    ):
        await app.sendGroupMessage(
            group,
            messageChain.create(
                [
                    Plain(
                        f"{member.name}({member.id}) 侠士的奇遇查询结果如下({sum(member_qiyu_trigger_info.values())}/{len(member_qiyu_trigger_info)}):\n"
                    ),
                    Plain(
                        "\n".join(
                            [
                                f"{qiyu}: 已触发" if status else f"{qiyu}: 未触发"
                                for qiyu, status in member_qiyu_trigger_info.items()
                            ]
                        )
                    ),
                ]
            ),
        )

    async def task(app, group, member, messageChain):
        member_qiyu = query_member_qiyu()
        qiyu_trigger_info = generate_member_qiyu_trigger_info(member_qiyu)
        loop_manager.create_task(
            send_member_qiyu_trigger_info(
                app, group, member, messageChain, qiyu_trigger_info
            ),
            "MainThread",
        )

    loop_manager.create_task(task(app, group, member, messageChain), "QiyuThread")
