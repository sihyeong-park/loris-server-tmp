import zmq
import json
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from multiprocessing import Process, Queue


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


app = FastAPI()
manager = ConnectionManager()

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect(f'tcp://localhost:8888')


@app.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            socket.send(data.encode())

            msg = socket.recv()
            await manager.send_personal_message(msg, websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast(f"Client left the chat")