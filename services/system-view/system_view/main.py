import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from system_view import __version__


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("system_view.main:app", host="0.0.0.0", port=8000, reload=True)
