from fastapi import FastAPI
from .routers import properties, auth  # Import routers
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

FRONTEND_DIR = "frontend"

app = FastAPI(title="Real Estate Search API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount the static directory to serve HTML, CSS, and JS files
# This makes files accessible at http://localhost:8000/static/search_list.html and /static/add_update.html
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# Root route to serve the main HTML file. Changed from index.html to search_list.html
@app.get("/", include_in_schema=False)
async def serve_index():
    # This ensures that when the user accesses the root URL (e.g., http://localhost:8000), 
    # they are served the search page.
    return FileResponse(f"{FRONTEND_DIR}/search_list.html")

# Register routers
app.include_router(properties.router) #gets all the routes from properties.py and includes them in the main app
app.include_router(auth.router) #gets all the auth routes (signup, login, /me)
