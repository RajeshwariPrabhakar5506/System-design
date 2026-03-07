import os
from dotenv import load_dotenv
from openai import OpenAI

EnvPath = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(EnvPath)

