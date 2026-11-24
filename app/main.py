"""FastAPI application: trace-ordering service for one seismic survey line.

Endpoints
---------
POST /line/initialize
    Create the line with a total trace count. Version 1 keeps traces
    1..total_traces in original order.

POST /line/move
    Submit a move against a base version: cut positions [start, end]
    (inclusive) and insert the segment right after ``insert_after``
    (0 = front, total = end). Segment-internal order is preserved.

GET  /line/lookup
    Given a published version and a position, return the original trace
    number plus its current version and position.

GET  /line/traces/{original_trace}
    Where an original trace currently sits.

GET  /line/versions
    Published version history.

GET  /line
    Line metadata.
"""
from __future__ import annotations

import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .storage import (
    InvalidMoveError,
    LineRepository,
    NotFoundError,
    StaleVersionError,
)

DB_PATH = os.environ.get("DB_PATH", "line.db")

app = FastAPI(
    title="Seismic line trace-ordering API",
    version="1.0.0",
    description=(
        "Maintains one survey line's trace ordering across versions. "
        "Moves are versioned; every published version stays queryable."
    ),
)
repo = LineRepository(DB_PATH)


@app.exception_handler(StaleVersionError)
async def _stale_handler(_: Request, exc: StaleVersionError) -> JSONResponse:
    return JSONResponse(status_code=409, content={"detail": str(exc)})


@app.exception_handler(InvalidMoveError)
async def _invalid_handler(_: Request, exc: InvalidMoveError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"detail": str(exc)})


@app.exception_handler(NotFoundError)
async def _not_found_handler(_: Request, exc: NotFoundError) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


class InitializeRequest(BaseModel):
    total_traces: int = Field(
        ..., ge=1, description="Total number of traces; original ids are 1..N."
    )


class MoveRequest(BaseModel):
    base_version: int = Field(
        ..., ge=1,
        description="Version number the client based this move on. "
                    "Must equal the current version or the move is rejected.",
    )
    start: int = Field(
        ..., ge=1,
        description="First position of the segment to move (inclusive).",
    )
    end: int = Field(
        ..., ge=1,
        description="Last position of the segment to move (inclusive).",
    )
    insert_after: int = Field(
        ..., ge=0,
        description="Position after which the segment is inserted in the "
                    "resulting order: 0 = front, total_traces = end.",
    )


@app.post("/line/initialize", status_code=201)
def initialize_line(payload: InitializeRequest) -> dict:
    version = repo.initialize(payload.total_traces)
    return {
        "version": version,
        "total_traces": payload.total_traces,
        "current_root": repo.get_meta()["current_root"],
    }


@app.post("/line/move")
def move_segment(payload: MoveRequest) -> dict:
    result = repo.move_segment(
        base_version=payload.base_version,
        start=payload.start,
        end=payload.end,
        insert_after=payload.insert_after,
    )
    return {
        "version": result["version"],
        "base_version": payload.base_version,
        "moved": {"start": payload.start, "end": payload.end},
        "insert_after": payload.insert_after,
        "total_traces": result["total_traces"],
    }


@app.get("/line/lookup")
def lookup_position(version: int, position: int) -> dict:
    return repo.lookup_position(version, position)


@app.get("/line/traces/{original_trace}")
def lookup_trace(original_trace: int) -> dict:
    return repo.lookup_trace(original_trace)


@app.get("/line/versions")
def list_versions() -> dict:
    return {"versions": repo.list_versions()}


@app.get("/line")
def line_meta() -> dict:
    meta = repo.get_meta()
    return {
        "total_traces": meta["total_traces"],
        "current_version": meta["current_version"],
        "current_root": meta["current_root"],
    }


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "initialized": repo.is_initialized()}
