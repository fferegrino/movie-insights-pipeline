import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from system_view import __version__
from system_view.kafka_connections import KafkaConnections
from system_view.settings import SystemViewSettings
from system_view.websocket_manager import WebSocketManager

logger = logging.getLogger(__name__)

settings = SystemViewSettings()

kafka_connections = KafkaConnections(settings)
websocket_manager = WebSocketManager(kafka_connections)


@asynccontextmanager
async def lifespan(app: FastAPI):
    websocket_manager.start()
    await kafka_connections.connect()
    yield
    await kafka_connections.disconnect()
    websocket_manager.stop()


app = FastAPI(
    title="System View", description="Movie insights pipeline system view", version=__version__, lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Mount static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", response_class=HTMLResponse)
async def get_index():
    """Serve the main HTML page."""
    html_path = os.path.join(static_dir, "index.html")
    with open(html_path) as f:
        return HTMLResponse(content=f.read())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/version")
async def version():
    return {"version": __version__}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await websocket_manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
        websocket_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        websocket_manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("system_view.main:app", host="0.0.0.0", port=8000, reload=True)
