from fastapi import FastAPI
from routers import properties # Import the properties router
from fastapi.middleware.cors import CORSMiddleware 
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Real Estate Search API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Root
@app.get("/")
def root():
    return {"message": "hi shaurya, congrats on not breaking anything"}

# Register routers
app.include_router(properties.router) #gets all the routes from properties.py and includes them in the main app
