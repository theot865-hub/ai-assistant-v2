import json
from llm import ask
import tools

SYSTEM = """
You are a helpful local AI assistant.

IMPORTANT TOOL RULES:
- If you want to use a tool, you MUST reply with ONLY a single valid JSON object on one line.
- No extra words, no markdown, no backticks.
- Otherwise reply normally.

Available tools (JSON formats):
{"tool":"write_file","rel_path":"notes/test.txt","content":"hello world"}
{"tool":"read_file","rel_path":"notes/test.txt"}
{"tool":"run_safe_cmd","cmd":["ls"]}
{"tool":"memory_set","key":"name","value":"Theo"}
{"tool":"memory_get","key":"name"}
{"tool":"memory_list"}
{"tool":"search_web","query":"Victoria BC pressure washing marketing ideas","max_results":5}

Memory behavior:
- If the user tells you their name, store it with memory_set key="name".
- If the user asks who they are / what their name is, use memory_get key="name".
- Before answering normal questions, if name exists, use it naturally.
"""

def parse_tool(text: str):
    text = text.strip()

    # Try direct JSON first
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and "tool" in obj:
            return obj
    except Exception:
        pass

    # Otherwise find first JSON object inside the text (handles model being messy)
    start = text.find("{")
    while start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    chunk = text[start:i+1]
                    try:
                        obj = json.loads(chunk)
                        if isinstance(obj, dict) and "tool" in obj:
                            return obj
                    except Exception:
                        break
        start = text.find("{", start + 1)

    return None

def run_tool(call: dict) -> str:
    t = call.get("tool")

    if t == "write_file":
        return tools.write_file(call["rel_path"], call.get("content", ""))

    if t == "read_file":
        return tools.read_file(call["rel_path"])

    if t == "run_safe_cmd":
        return tools.run_safe_cmd(call.get("cmd", []))

    if t == "memory_set":
        return tools.memory_set(call["key"], call.get("value", ""))

    if t == "memory_get":
        return tools.memory_get(call["key"])

    if t == "memory_list":
        return tools.memory_list()

    if t == "search_web":
        return tools.search_web(call["query"], call.get("max_results", 5))

    return f"Unknown tool: {t}"

def main():
    print("Assistant v2 ready (type 'exit' to quit)")

    while True:
        user = input("You: ").strip()
        if user.lower() == "exit":
            break

        # Pull name if it exists (quietly) so the model can personalize answers
        name = tools.memory_get("name")
        name_line = ""
        if name and name != "(no value)":
            name_line = f"\nKnown user name: {name}\n"

        # Agent loop: model can call tools multiple times
        scratch = ""
        for _ in range(6):  # max 6 tool hops per user message
            prompt = SYSTEM + name_line + scratch + "\nUser: " + user
            response = ask(prompt)

            call = parse_tool(response)
            if not call:
                print("AI:", response)
                break

            result = run_tool(call)
            # Feed tool result back into the next model call
            scratch += f"\nTOOL_CALL: {json.dumps(call)}\nTOOL_RESULT:\n{result}\n"
        else:
            print("AI: (stopped: too many tool calls)")

if __name__ == "__main__":
    main()