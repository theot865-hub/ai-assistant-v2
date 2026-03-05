import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3:latest"

def ask(prompt: str) -> str:
    payload = {"model": MODEL, "prompt": prompt, "stream": False}
    r = requests.post(OLLAMA_URL, json=payload, timeout=180)
    r.raise_for_status()
    return r.json()["response"]