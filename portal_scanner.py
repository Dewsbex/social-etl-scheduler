import os
import asyncio
import json
from langchain_google_genai import ChatGoogleGenerativeAI
from browser_use import Agent

async def scan_school_portal():
    """
    Scans the configured School Portal for new events using Browser Use.
    Returns a list of event dictionaries similar to the email extractor.
    """
    portal_url = os.getenv("SCHOOL_PORTAL_URL")
    if not portal_url:
        print("Logistics Officer: SCHOOL_PORTAL_URL not set. Skipping portal scan.")
        return []

    print(f"Logistics Officer: Scanning School Portal at {portal_url}...")
    
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Logistics Officer: GEMINI_API_KEY not set. Cannot use LLM for portal scan.")
        return []

    # Initialize Gemini
    llm = ChatGoogleGenerativeAI(model="gemini-1.5-pro", google_api_key=api_key)

    task = f"""
    1. Go to {portal_url}
    2. Look for a "Calendar" or "Events" section. If you need to login, stop and report "Login Required" as an error (we are not handling auth yet).
    3. Extract all future events listed.
    4. For each event, extract: Title, Date, Time, Location, Description.
    5. Return the result as a strict JSON list of objects with keys: event_title, start_time, end_time, location, description, subjects.
    
    Example format:
    [
        {{
            "event_title": "Football Match",
            "start_time": "2023-10-25T14:00:00",
            "end_time": "2023-10-25T16:00:00",
            "location": "School Field",
            "description": "vs St. Mary's",
            "subjects": ["Tristan"]
        }}
    ]
    """

    try:
        # Use default browser configuration
        agent = Agent(task=task, llm=llm)
        result = await agent.run()
        
        # Parse the result. The agent returns a History object, we need the final output.
        # This part depends heavily on how the agent is prompted to return data.
        # Ideally, we ask it to Output strictly JSON in its final response.
        
        final_output = result.final_result() 
        
        # Cleanup JSON if needed (Markdown fencing)
        if "```json" in final_output:
            final_output = final_output.split("```json")[1].split("```")[0].strip()
        elif "```" in final_output:
             final_output = final_output.split("```")[1].split("```")[0].strip()
             
        events = json.loads(final_output)
        
        # Tag source
        for event in events:
            event['source'] = 'portal'
            
        print(f"Logistics Officer: Found {len(events)} events from Portal.")
        return events

    except Exception as e:
        print(f"Logistics Officer: Portal Scan Failed: {e}")
        return []

if __name__ == "__main__":
    # Test run
    from dotenv import load_dotenv
    load_dotenv()
    print("Running Portal Scanner Test...")
    events = asyncio.run(scan_school_portal())
    print(json.dumps(events, indent=2))
