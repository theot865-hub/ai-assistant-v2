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
def handle_goal_mode(user: str) -> str:
    goal = user.replace("goal:", "", 1).strip()

    plan_prompt = f"""
Create a short step-by-step plan for this goal:

{goal}

Rules:
- 3 to 6 steps
- plain text only
- practical actions
"""

    plan = ask(plan_prompt)

    research_results = ""
    extracted_leads = ""

    if "research" in goal.lower():
        research_results = tools.search_web(goal, 5)
        tools.write_file("jobs/latest_research.json", research_results)

        extract_prompt = f"""
You are extracting leads from web research.

Goal:
{goal}

Raw search results:
{research_results}

Return a plain text lead list.
Each lead should be on its own line like this:

Name | Company | Website

If some fields are missing, still include the lead with whatever is available.
Do not use JSON.
Do not add explanations.
"""

        extracted_leads = ask(extract_prompt)
        tools.write_file("leads/realtors.txt", extracted_leads)

    tools.write_file("jobs/latest_plan.txt", f"GOAL:\n{goal}\n\nPLAN:\n{plan}")

    if research_results:
        return (
            "Plan saved to workspace/jobs/latest_plan.txt\n"
            "Research saved to workspace/jobs/latest_research.json\n"
            "Lead list saved to workspace/leads/realtors.txt\n\n"
            f"{plan}"
        )

    return f"Plan saved to workspace/jobs/latest_plan.txt\n\n{plan}"
def main():
    print("Assistant v2 ready (type 'exit' to quit)")

    while True:
        user = input("You: ").strip()
        if user.lower() == "exit":
            break
        if user.lower().startswith("goal:"):
            result = handle_goal_mode(user)
            print("AI:", result)
            continue
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