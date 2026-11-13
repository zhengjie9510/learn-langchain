"""同步 LangGraph 事件流 -> FastAPI SSE（一个文件，无前端页面）。

安装：pip install 'langgraph>=1.2,<2' fastapi uvicorn
启动：python '如何用同步事件流通过SSE查看LangGraph节点执行.py'
测试：curl -N 'http://127.0.0.1:8000/stream?question=SSE是什么'
"""

import json
import time
from typing import TypedDict
from uuid import uuid4

import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.stream import TasksTransformer


# 1. 工作流数据
class State(TypedDict, total=False):
    question: str
    cleaned: str
    facts: str
    draft: str
    answer: str


# 2. 四个节点，全部是普通同步函数
def clean(state: State):
    time.sleep(1)
    return {"cleaned": state["question"].strip()}


def retrieve(state: State):
    time.sleep(1)
    return {"facts": "SSE 让服务器通过一个 HTTP 连接持续推送事件。"}


def generate(state: State):
    time.sleep(1)
    return {"draft": f"问题：{state['cleaned']}；回答：{state['facts']}"}


def review(state: State):
    time.sleep(1)
    return {"answer": state["draft"] + "（已检查）"}


# 3. 组装工作流
builder = StateGraph(State)
builder.add_node("整理问题", clean)
builder.add_node("查找资料", retrieve)
builder.add_node("生成回答", generate)
builder.add_node("检查结果", review)

builder.add_edge(START, "整理问题")
builder.add_edge("整理问题", "查找资料")
builder.add_edge("查找资料", "生成回答")
builder.add_edge("生成回答", "检查结果")
builder.add_edge("检查结果", END)

# tasks 事件需要 checkpointer；每次请求使用独立的 thread_id
graph = builder.compile(checkpointer=MemorySaver())

# 4. SSE 接口：同步读取 LangGraph 自动产生的任务事件
app = FastAPI()


@app.get("/stream")
def stream(question: str = "什么是 SSE？"):
    def events():
        try:
            run = graph.stream_events(
                {"question": question},
                config={"configurable": {"thread_id": str(uuid4())}},
                version="v3",  # 同步 stream_events 使用 v3
                transformers=[TasksTransformer],
            )

            for task in run.tasks:
                # 每个节点会有“开始”和“结果”两类任务事件
                if "input" in task:
                    event_name = "node_start"
                    data = {"node": task["name"], "status": "开始"}
                else:
                    event_name = "node_end"
                    data = {
                        "node": task["name"],
                        "status": "失败" if task.get("error") else "完成",
                        "output": task.get("result"),
                        "error": task.get("error"),
                    }

                yield (
                    f"event: {event_name}\n"
                    f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
                )

            yield 'event: workflow_done\ndata: {"message":"工作流结束"}\n\n'
        except Exception as exc:
            data = json.dumps({"error": str(exc)}, ensure_ascii=False)
            yield f"event: workflow_error\ndata: {data}\n\n"

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    # 只启动服务，不自动打开浏览器
    uvicorn.run(app, host="127.0.0.1", port=8000)
