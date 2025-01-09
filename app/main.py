from fastapi import FastAPI
from app.telegram_client import telegram_client
from contextlib import asynccontextmanager
from app.routers import messages, groups
from app.services.telegram_listener import update_listener


@asynccontextmanager
async def app_lifespan(app: FastAPI):
    try:
        # Start the Telegram client
        await telegram_client.start()
        print("Telegram client started.")

        # Initialize the listener with current active groups
        await update_listener()
        print("Listener initialized at app startup.")

        yield

    finally:
        # Cleanup on shutdown
        print("Shutting down Telegram client...")
        await telegram_client.disconnect()

app = FastAPI(lifespan=app_lifespan)

# Include routers for API endpoints
app.include_router(messages.router)
app.include_router(groups.router)


@app.get("/")
async def root():
    return {"message": "Welcome to Telegram Monitor API"}
